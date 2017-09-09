package cn.ms.coon.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import cn.ms.coon.CoonListener;
import cn.ms.coon.support.FailbackMreg;
import cn.ms.coon.support.common.Consts;
import cn.ms.coon.support.common.MregCommon;
import cn.ms.coon.support.common.NamedThreadFactory;
import cn.ms.coon.support.exception.MregException;
import cn.ms.neural.NURL;

public class RedisMreg extends FailbackMreg {

	private static final Logger logger = LoggerFactory.getLogger(RedisMreg.class);

    private final String root;
    private boolean replicate;
    private final int expirePeriod;
    private final int reconnectPeriod;
    private volatile boolean admin = false;
    private final static String DEFAULT_ROOT = "ms";
	private static final int DEFAULT_REDIS_PORT = 6379;
    private final Map<String, JedisPool> jedisPools = new ConcurrentHashMap<String, JedisPool>();
    private final ConcurrentMap<String, Notifier> notifiers = new ConcurrentHashMap<String, Notifier>();
    private final ScheduledFuture<?> expireFuture;
    private final ScheduledExecutorService expireExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("RedisMregExpireTimer", true));

    
    public RedisMreg(NURL nurl) {
        super(nurl);
        if (nurl.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setTestOnBorrow(nurl.getParameter("test.on.borrow", true));
        config.setTestOnReturn(nurl.getParameter("test.on.return", false));
        config.setTestWhileIdle(nurl.getParameter("test.while.idle", false));
        if (nurl.getParameter("max.idle", 0) > 0)
            config.setMaxIdle(nurl.getParameter("max.idle", 0));
        if (nurl.getParameter("min.idle", 0) > 0)
            config.setMinIdle(nurl.getParameter("min.idle", 0));
        if (nurl.getParameter("max.total", 0) > 0)
            config.setMaxTotal(nurl.getParameter("max.total", 0));
        if (nurl.getParameter("max.wait.millis", nurl.getParameter("timeout", 0)) > 0)
            config.setMaxWaitMillis(nurl.getParameter("max.wait.millis", nurl.getParameter("timeout", 0)));
        if (nurl.getParameter("num.tests.per.eviction.run", 0) > 0)
            config.setNumTestsPerEvictionRun(nurl.getParameter("num.tests.per.eviction.run", 0));
        if (nurl.getParameter("time.between.eviction.runs.millis", 0) > 0)
            config.setTimeBetweenEvictionRunsMillis(nurl.getParameter("time.between.eviction.runs.millis", 0));
        if (nurl.getParameter("min.evictable.idle.time.millis", 0) > 0)
            config.setMinEvictableIdleTimeMillis(nurl.getParameter("min.evictable.idle.time.millis", 0));

        String cluster = nurl.getParameter("cluster", "failover");
        if (! "failover".equals(cluster) && ! "replicate".equals(cluster)) {
            throw new IllegalArgumentException("Unsupported redis cluster: " + cluster + ". The redis cluster only supported failover or replicate.");
        }
        replicate = "replicate".equals(cluster);

        List<String> addresses = new ArrayList<String>();
        addresses.add(nurl.getAddress());
        String[] backups = nurl.getParameter(Consts.BACKUP_KEY, new String[0]);
        if (backups != null && backups.length > 0) {
            addresses.addAll(Arrays.asList(backups));
        }

        // 增加Redis密码支持
        String password = nurl.getPassword();
        for (String address : addresses) {
            int i = address.indexOf(':');
            String host;
            int port;
            if (i > 0) {
                host = address.substring(0, i);
                port = Integer.parseInt(address.substring(i + 1));
            } else {
                host = address;
                port = DEFAULT_REDIS_PORT;
            }
            if (MregCommon.isEmpty(password)) {
                this.jedisPools.put(address, new JedisPool(config, host, port,
                        nurl.getParameter(Consts.TIMEOUT_KEY, Consts.DEFAULT_TIMEOUT)));
            } else {
                // 使用密码连接。  此处要求备用redis与主要redis使用相同的密码
                this.jedisPools.put(address, new JedisPool(config, host, port,
                        nurl.getParameter(Consts.TIMEOUT_KEY, Consts.DEFAULT_TIMEOUT), password));
            }
        }

        this.reconnectPeriod = nurl.getParameter(Consts.REGISTRY_RECONNECT_PERIOD_KEY, Consts.DEFAULT_REGISTRY_RECONNECT_PERIOD);
        String group = nurl.getParameter(Consts.GROUP_KEY, DEFAULT_ROOT);
        if (! group.startsWith(Consts.PATH_SEPARATOR)) {
            group = Consts.PATH_SEPARATOR + group;
        }
        if (! group.endsWith(Consts.PATH_SEPARATOR)) {
            group = group + Consts.PATH_SEPARATOR;
        }
        this.root = group;

        this.expirePeriod = nurl.getParameter(Consts.SESSION_TIMEOUT_KEY, Consts.DEFAULT_SESSION_TIMEOUT);
        this.expireFuture = expireExecutor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    deferExpired(); // 延长过期时间
                } catch (Throwable t) { // 防御性容错
                    logger.error("Unexpected exception occur at defer expire time, cause: " + t.getMessage(), t);
                }
            }
        }, expirePeriod / 2, expirePeriod / 2, TimeUnit.MILLISECONDS);
    }

    private void deferExpired() {
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    for (NURL nurl : new HashSet<NURL>(getRegistered())) {
                        if (nurl.getParameter(Consts.DYNAMIC_KEY, true)) {
                            String key = toCategoryPath(nurl);
                            if (jedis.hset(key, nurl.toFullString(), String.valueOf(System.currentTimeMillis() + expirePeriod)) == 1) {
                                jedis.publish(key, Consts.REGISTER);
                            }
                        }
                    }
                    if (admin) {
                        clean(jedis);
                    }
                    if (!replicate) {
                        break;//  如果服务器端已同步数据，只需写入单台机器
                    }
                } catch (JedisConnectionException e){
                	logger.error("Jedis Connection Exception", e);
                } finally {
                    if(jedis != null){
                    	jedis.close();
                    }
                }
            } catch (Throwable t) {
                logger.warn("Failed to write provider heartbeat to redis registry. registry: " + entry.getKey() + ", cause: " + t.getMessage(), t);
            }
        }
    }

    // 监控中心负责删除过期脏数据
    private void clean(Jedis jedis) {
        Set<String> keys = jedis.keys(root + Consts.ANY_VALUE);
        if (keys != null && keys.size() > 0) {
            for (String key : keys) {
                Map<String, String> values = jedis.hgetAll(key);
                if (values != null && values.size() > 0) {
                    boolean delete = false;
                    long now = System.currentTimeMillis();
                    for (Map.Entry<String, String> entry : values.entrySet()) {
                    	NURL nurl = NURL.valueOf(entry.getKey());
                        if (nurl.getParameter(Consts.DYNAMIC_KEY, true)) {
                            long expire = Long.parseLong(entry.getValue());
                            if (expire < now) {
                                jedis.hdel(key, entry.getKey());
                                delete = true;
                                if (logger.isWarnEnabled()) {
                                    logger.warn("Delete expired key: " + key + " -> value: " + entry.getKey() + ", expire: " + new Date(expire) + ", now: " + new Date(now));
                                }
                            }
                        }
                    }
                    if (delete) {
                        jedis.publish(key, Consts.UNREGISTER);
                    }
                }
            }
        }
    }

    @Override
    public boolean isAvailable() {
        for (JedisPool jedisPool : jedisPools.values()) {
            Jedis jedis = jedisPool.getResource();
            try {
                if (jedis.isConnected()) {
                    return true; // 至少需单台机器可用
                }
            } catch (JedisConnectionException e) {
            	logger.error("Jedis Connection Exception", e);
            } finally {
            	if(jedis != null){
                	jedis.close();
                }
            }
        }
        return false;
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            expireFuture.cancel(true);
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        try {
            for (Notifier notifier : notifiers.values()) {
                notifier.shutdown();
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                jedisPool.destroy();
            } catch (Throwable t) {
                logger.warn("Failed to destroy the redis registry client. registry: " + entry.getKey() + ", cause: " + t.getMessage(), t);
            }
        }
    }

    @Override
    public void doRegister(NURL nurl) {
        String key = toCategoryPath(nurl);
        String value = nurl.toFullString();
        String expire = String.valueOf(System.currentTimeMillis() + expirePeriod);
        boolean success = false;
        MregException exception = null;
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    jedis.hset(key, value, expire);
                    jedis.publish(key, Consts.REGISTER);
                    success = true;
                    if (! replicate) {
                        break; //  如果服务器端已同步数据，只需写入单台机器
                    }
                } catch (JedisConnectionException e){
                	logger.error("Jedis Connection Exception", e);
                } finally {
                	if(jedis != null){
                    	jedis.close();
                    }
                }
            } catch (Throwable t) {
                exception = new MregException("Failed to register service to redis registry. registry: " + entry.getKey() + ", service: " + nurl + ", cause: " + t.getMessage(), t);
            }
        }
        if (exception != null) {
            if (success) {
                logger.warn(exception.getMessage(), exception);
            } else {
                throw exception;
            }
        }
    }

    @Override
    public void doUnregister(NURL nurl) {
        String key = toCategoryPath(nurl);
        String value = nurl.toFullString();
        MregException exception = null;
        boolean success = false;
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    jedis.hdel(key, value);
                    jedis.publish(key, Consts.UNREGISTER);
                    success = true;
                    if (! replicate) {
                        break; //  如果服务器端已同步数据，只需写入单台机器
                    }
                } catch (JedisConnectionException e){
                	logger.error("Jedis Connection Exception", e);
                } finally {
                	if(jedis != null){
                    	jedis.close();
                    }
                }
            } catch (Throwable t) {
                exception = new MregException("Failed to unregister service to redis registry. registry: " + entry.getKey() + ", service: " + nurl + ", cause: " + t.getMessage(), t);
            }
        }
        if (exception != null) {
            if (success) {
                logger.warn(exception.getMessage(), exception);
            } else {
                throw exception;
            }
        }
    }

    @Override
    public void doSubscribe(final NURL nurl, final CoonListener<NURL> listener) {
        String service = toServicePath(nurl);
        Notifier notifier = notifiers.get(service);
        if (notifier == null) {
            Notifier newNotifier = new Notifier(service);
            notifiers.putIfAbsent(service, newNotifier);
            notifier = notifiers.get(service);
            if (notifier == newNotifier) {
                notifier.start();
            }
        }
        boolean success = false;
        MregException exception = null;
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    if (service.endsWith(Consts.ANY_VALUE)) {
                        admin = true;
                        Set<String> keys = jedis.keys(service);
                        if (keys != null && keys.size() > 0) {
                            Map<String, Set<String>> serviceKeys = new HashMap<String, Set<String>>();
                            for (String key : keys) {
                                String serviceKey = toServicePath(key);
                                Set<String> sk = serviceKeys.get(serviceKey);
                                if (sk == null) {
                                    sk = new HashSet<String>();
                                    serviceKeys.put(serviceKey, sk);
                                }
                                sk.add(key);
                            }
                            for (Set<String> sk : serviceKeys.values()) {
                                doNotify(jedis, sk, nurl, Arrays.asList(listener));
                            }
                        }
                    } else {
                        doNotify(jedis, jedis.keys(service + Consts.PATH_SEPARATOR + Consts.ANY_VALUE), nurl, Arrays.asList(listener));
                    }
                    success = true;
                    break; // 只需读一个服务器的数据
                } catch (JedisConnectionException e){
                	logger.error("Jedis Connection Exception", e);
                } finally {
                	if(jedis != null){
                    	jedis.close();
                    }
                }
            } catch(Throwable t) { // 尝试下一个服务器
                exception = new MregException("Failed to subscribe service from redis registry. registry: " + entry.getKey() + ", service: " + nurl + ", cause: " + t.getMessage(), t);
            }
        }
        if (exception != null) {
            if (success) {
                logger.warn(exception.getMessage(), exception);
            } else {
                throw exception;
            }
        }
    }

    @Override
    public void doUnsubscribe(NURL nurl, CoonListener<NURL> listener) {
    }

    private void doNotify(Jedis jedis, String key) {
        for (Map.Entry<NURL, Set<CoonListener<NURL>>> entry : new HashMap<NURL, Set<CoonListener<NURL>>>(getSubscribed()).entrySet()) {
            doNotify(jedis, Arrays.asList(key), entry.getKey(), new HashSet<CoonListener<NURL>>(entry.getValue()));
        }
    }

    private void doNotify(Jedis jedis, Collection<String> keys, NURL nurl, Collection<CoonListener<NURL>> listeners) {
        if (keys == null || keys.size() == 0
                || listeners == null || listeners.size() == 0) {
            return;
        }
        long now = System.currentTimeMillis();
        List<NURL> result = new ArrayList<NURL>();
        List<String> categories = Arrays.asList(nurl.getParameter(Consts.CATEGORY_KEY, new String[0]));
        String consumerService = nurl.getServiceInterface();
        for (String key : keys) {
            if (! Consts.ANY_VALUE.equals(consumerService)) {
                String prvoiderService = toServiceName(key);
                if (! prvoiderService.equals(consumerService)) {
                    continue;
                }
            }
            String category = toCategoryName(key);
            if (! categories.contains(Consts.ANY_VALUE) && ! categories.contains(category)) {
                continue;
            }
            List<NURL> nurls = new ArrayList<NURL>();
            Map<String, String> values = jedis.hgetAll(key);
            if (values != null && values.size() > 0) {
                for (Map.Entry<String, String> entry : values.entrySet()) {
                	NURL u = NURL.valueOf(entry.getKey());
                    if (! u.getParameter(Consts.DYNAMIC_KEY, true)
                            || Long.parseLong(entry.getValue()) >= now) {
                        if (MregCommon.isMatch(nurl, u)) {
                            nurls.add(u);
                        }
                    }
                }
            }
            if (nurls.isEmpty()) {
                nurls.add(nurl.setProtocol(Consts.EMPTY_PROTOCOL)
                        .setAddress(Consts.ANYHOST_VALUE)
                        .setPath(toServiceName(key))
                        .addParameter(Consts.CATEGORY_KEY, category));
            }
            result.addAll(nurls);
            if (logger.isInfoEnabled()) {
                logger.info("redis notify: " + key + " = " + nurls);
            }
        }
        if (result == null || result.size() == 0) {
            return;
        }
        for (CoonListener<NURL> listener : listeners) {
            notify(nurl, listener, result);
        }
    }

    private String toServiceName(String categoryPath) {
        String servicePath = toServicePath(categoryPath);
        return servicePath.startsWith(root) ? servicePath.substring(root.length()) : servicePath;
    }

    private String toCategoryName(String categoryPath) {
        int i = categoryPath.lastIndexOf(Consts.PATH_SEPARATOR);
        return i > 0 ? categoryPath.substring(i + 1) : categoryPath;
    }

    private String toServicePath(String categoryPath) {
        int i;
        if (categoryPath.startsWith(root)) {
            i = categoryPath.indexOf(Consts.PATH_SEPARATOR, root.length());
        } else {
            i = categoryPath.indexOf(Consts.PATH_SEPARATOR);
        }
        return i > 0 ? categoryPath.substring(0, i) : categoryPath;
    }

    private String toServicePath(NURL nurl) {
        return root + nurl.getServiceInterface();
    }

    private String toCategoryPath(NURL nurl) {
        return toServicePath(nurl) + Consts.PATH_SEPARATOR + nurl.getParameter(Consts.CATEGORY_KEY, Consts.DEFAULT_CATEGORY);
    }

    private class NotifySub extends JedisPubSub {

        private final JedisPool jedisPool;

        public NotifySub(JedisPool jedisPool) {
            this.jedisPool = jedisPool;
        }

        @Override
        public void onMessage(String key, String msg) {
            if (logger.isInfoEnabled()) {
                logger.info("redis event: " + key + " = " + msg);
            }
            if (msg.equals(Consts.REGISTER)
                    || msg.equals(Consts.UNREGISTER)) {
                try {
                    Jedis jedis = jedisPool.getResource();
                    try {
                        doNotify(jedis, key);
                    } catch (JedisConnectionException e){
                    	logger.error("Jedis Connection Exception", e);
                    } finally {
                    	if(jedis != null){
                        	jedis.close();
                        }
                    }
                } catch (Throwable t) { // TODO 通知失败没有恢复机制保障
                    logger.error(t.getMessage(), t);
                }
            }
        }

        @Override
        public void onPMessage(String pattern, String key, String msg) {
            onMessage(key, msg);
        }

        @Override
        public void onSubscribe(String key, int num) {
        }

        @Override
        public void onPSubscribe(String pattern, int num) {
        }

        @Override
        public void onUnsubscribe(String key, int num) {
        }

        @Override
        public void onPUnsubscribe(String pattern, int num) {
        }

    }

    private class Notifier extends Thread {

        private final String service;

        private volatile Jedis jedis;

        private volatile boolean first = true;

        private volatile boolean running = true;

        private final AtomicInteger connectSkip = new AtomicInteger();

        private final AtomicInteger connectSkiped = new AtomicInteger();

        private final Random random = new Random();

        private volatile int connectRandom;

        private void resetSkip() {
            connectSkip.set(0);
            connectSkiped.set(0);
            connectRandom = 0;
        }

        private boolean isSkip() {
            int skip = connectSkip.get(); // 跳过次数增长
            if (skip >= 10) { // 如果跳过次数增长超过10，取随机数
                if (connectRandom == 0) {
                    connectRandom = random.nextInt(10);
                }
                skip = 10 + connectRandom;
            }
            if (connectSkiped.getAndIncrement() < skip) { // 检查跳过次数
                return true;
            }
            connectSkip.incrementAndGet();
            connectSkiped.set(0);
            connectRandom = 0;
            return false;
        }

        public Notifier(String service) {
            super.setDaemon(true);
            super.setName("MsRedisSubscribe");
            this.service = service;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    if (! isSkip()) {
                        try {
                            for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
                                JedisPool jedisPool = entry.getValue();
                                try {
                                    jedis = jedisPool.getResource();
                                    try {
                                        if (service.endsWith(Consts.ANY_VALUE)) {
                                            if (! first) {
                                                first = false;
                                                Set<String> keys = jedis.keys(service);
                                                if (keys != null && keys.size() > 0) {
                                                    for (String s : keys) {
                                                        doNotify(jedis, s);
                                                    }
                                                }
                                                resetSkip();
                                            }
                                            jedis.psubscribe(new NotifySub(jedisPool), service); // 阻塞
                                        } else {
                                            if (! first) {
                                                first = false;
                                                doNotify(jedis, service);
                                                resetSkip();
                                            }
                                            jedis.psubscribe(new NotifySub(jedisPool), service + Consts.PATH_SEPARATOR + Consts.ANY_VALUE); // 阻塞
                                        }
                                        break;
                                    } finally {
                                    	if(jedis != null){
                                        	jedis.close();
                                        }
                                    }
                                } catch (Throwable t) { // 重试另一台
                                    logger.warn("Failed to subscribe service from redis registry. registry: " + entry.getKey() + ", cause: " + t.getMessage(), t);
                                    // 如果在单台redis的情况下，需要休息一会，避免空转占用过多cpu资源
                                    sleep(reconnectPeriod);
                                }
                            }
                        } catch (Throwable t) {
                            logger.error(t.getMessage(), t);
                            sleep(reconnectPeriod);
                        }
                    }
                } catch (Throwable t) {
                    logger.error(t.getMessage(), t);
                }
            }
        }

        public void shutdown() {
            try {
                running = false;
                jedis.disconnect();
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
        }
    }

}