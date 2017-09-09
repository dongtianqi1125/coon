package cn.ms.coon.support.mreg;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.CoonListener;
import cn.ms.coon.support.Consts;
import cn.ms.coon.support.NamedThreadFactory;
import cn.ms.coon.support.mreg.exception.SkipFailbackException;
import cn.ms.neural.NURL;
import cn.ms.neural.util.micro.ConcurrentHashSet;

public abstract class FailbackMreg extends AbstractMreg {

	private static final Logger logger = LoggerFactory.getLogger(FailbackMreg.class);
	
    // 定时任务执行器
    private final ScheduledExecutorService retryExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("MsRegistryFailedRetryTimer", true));
    // 失败重试定时器，定时检查是否有请求失败，如有，无限次重试
    private final ScheduledFuture<?> retryFuture;
    private final Set<NURL> failedRegistered = new ConcurrentHashSet<NURL>();
    private final Set<NURL> failedUnregistered = new ConcurrentHashSet<NURL>();
    private final ConcurrentMap<NURL, Set<CoonListener<NURL>>> failedSubscribed = new ConcurrentHashMap<NURL, Set<CoonListener<NURL>>>();
    private final ConcurrentMap<NURL, Set<CoonListener<NURL>>> failedUnsubscribed = new ConcurrentHashMap<NURL, Set<CoonListener<NURL>>>();
    private final ConcurrentMap<NURL, Map<CoonListener<NURL>, List<NURL>>> failedNotified = new ConcurrentHashMap<NURL, Map<CoonListener<NURL>, List<NURL>>>();

    public FailbackMreg(NURL url) {
        super(url);
        int retryPeriod = url.getParameter(Consts.REGISTRY_RETRY_PERIOD_KEY, Consts.DEFAULT_REGISTRY_RETRY_PERIOD);
        this.retryFuture = retryExecutor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    retry(); // 检测并连接注册中心
                } catch (Throwable t) { // 防御性容错
                    logger.error("Unexpected error occur at failed retry, cause: " + t.getMessage(), t);
                }
            }
        }, retryPeriod, retryPeriod, TimeUnit.MILLISECONDS);
    }

    public Future<?> getRetryFuture() {
        return retryFuture;
    }

    public Set<NURL> getFailedRegistered() {
        return failedRegistered;
    }

    public Set<NURL> getFailedUnregistered() {
        return failedUnregistered;
    }

    public Map<NURL, Set<CoonListener<NURL>>> getFailedSubscribed() {
        return failedSubscribed;
    }

    public Map<NURL, Set<CoonListener<NURL>>> getFailedUnsubscribed() {
        return failedUnsubscribed;
    }

    public Map<NURL, Map<CoonListener<NURL>, List<NURL>>> getFailedNotified() {
        return failedNotified;
    }

    private void addFailedSubscribed(NURL url, CoonListener<NURL> listener) {
        Set<CoonListener<NURL>> listeners = failedSubscribed.get(url);
        if (listeners == null) {
            failedSubscribed.putIfAbsent(url, new ConcurrentHashSet<CoonListener<NURL>>());
            listeners = failedSubscribed.get(url);
        }
        listeners.add(listener);
    }

    private void removeFailedSubscribed(NURL url, CoonListener<NURL> listener) {
        Set<CoonListener<NURL>> listeners = failedSubscribed.get(url);
        if (listeners != null) {
            listeners.remove(listener);
        }
        listeners = failedUnsubscribed.get(url);
        if (listeners != null) {
            listeners.remove(listener);
        }
        Map<CoonListener<NURL>, List<NURL>> notified = failedNotified.get(url);
        if (notified != null) {
            notified.remove(listener);
        }
    }

    @Override
    public void register(NURL url) {
        super.register(url);
        failedRegistered.remove(url);
        failedUnregistered.remove(url);
        try {
            // 向服务器端发送注册请求
            doRegister(url);
        } catch (Exception e) {
            Throwable t = e;

            // 如果开启了启动时检测，则直接抛出异常
            boolean check = getNurl().getParameter(Consts.CHECK_KEY, true)
                    && url.getParameter(Consts.CHECK_KEY, true)
                    && ! Consts.CONSUMER_PROTOCOL.equals(url.getProtocol());
            boolean skipFailback = t instanceof SkipFailbackException;
            if (check || skipFailback) {
                if(skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException("Failed to register " + url + " to mreg " + getNurl().getAddress() + ", cause: " + t.getMessage(), t);
            } else {
                logger.error("Failed to register " + url + ", waiting for retry, cause: " + t.getMessage(), t);
            }

            // 将失败的注册请求记录到失败列表，定时重试
            failedRegistered.add(url);
        }
    }

    @Override
    public void unregister(NURL url) {
        super.unregister(url);
        failedRegistered.remove(url);
        failedUnregistered.remove(url);
        try {
            // 向服务器端发送取消注册请求
            doUnregister(url);
        } catch (Exception e) {
            Throwable t = e;

            // 如果开启了启动时检测，则直接抛出异常
            boolean check = getNurl().getParameter(Consts.CHECK_KEY, true)
                    && url.getParameter(Consts.CHECK_KEY, true)
                    && ! Consts.CONSUMER_PROTOCOL.equals(url.getProtocol());
            boolean skipFailback = t instanceof SkipFailbackException;
            if (check || skipFailback) {
                if(skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException("Failed to unregister " + url + " to mreg " + getNurl().getAddress() + ", cause: " + t.getMessage(), t);
            } else {
                logger.error("Failed to uregister " + url + ", waiting for retry, cause: " + t.getMessage(), t);
            }

            // 将失败的取消注册请求记录到失败列表，定时重试
            failedUnregistered.add(url);
        }
    }

    @Override
    public void subscribe(NURL url, CoonListener<NURL> listener) {
        super.subscribe(url, listener);
        removeFailedSubscribed(url, listener);
        try {
            // 向服务器端发送订阅请求
            doSubscribe(url, listener);
        } catch (Exception e) {
            Throwable t = e;

            List<NURL> urls = getCacheUrls(url);
            if (urls != null && urls.size() > 0) {
                notify(url, listener, urls);
                logger.error("Failed to subscribe " + url + ", Using cached list: " + urls + " from cache file: " + getNurl().getParameter(Consts.FILE_KEY, System.getProperty("user.home") + "/dubbo-mreg-" + url.getHost() + ".cache") + ", cause: " + t.getMessage(), t);
            } else {
                // 如果开启了启动时检测，则直接抛出异常
                boolean check = getNurl().getParameter(Consts.CHECK_KEY, true)
                        && url.getParameter(Consts.CHECK_KEY, true);
                boolean skipFailback = t instanceof SkipFailbackException;
                if (check || skipFailback) {
                    if(skipFailback) {
                        t = t.getCause();
                    }
                    throw new IllegalStateException("Failed to subscribe " + url + ", cause: " + t.getMessage(), t);
                } else {
                    logger.error("Failed to subscribe " + url + ", waiting for retry, cause: " + t.getMessage(), t);
                }
            }

            // 将失败的订阅请求记录到失败列表，定时重试
            addFailedSubscribed(url, listener);
        }
    }

    @Override
    public void unsubscribe(NURL url, CoonListener<NURL> listener) {
        super.unsubscribe(url, listener);
        removeFailedSubscribed(url, listener);
        try {
            // 向服务器端发送取消订阅请求
            doUnsubscribe(url, listener);
        } catch (Exception e) {
            Throwable t = e;

            // 如果开启了启动时检测，则直接抛出异常
            boolean check = getNurl().getParameter(Consts.CHECK_KEY, true)
                    && url.getParameter(Consts.CHECK_KEY, true);
            boolean skipFailback = t instanceof SkipFailbackException;
            if (check || skipFailback) {
                if(skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException("Failed to unsubscribe " + url + " to mreg " + getNurl().getAddress() + ", cause: " + t.getMessage(), t);
            } else {
                logger.error("Failed to unsubscribe " + url + ", waiting for retry, cause: " + t.getMessage(), t);
            }

            // 将失败的取消订阅请求记录到失败列表，定时重试
            Set<CoonListener<NURL>> listeners = failedUnsubscribed.get(url);
            if (listeners == null) {
                failedUnsubscribed.putIfAbsent(url, new ConcurrentHashSet<CoonListener<NURL>>());
                listeners = failedUnsubscribed.get(url);
            }
            listeners.add(listener);
        }
    }

    @Override
    protected void notify(NURL url, CoonListener<NURL> listener, List<NURL> urls) {
        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        try {
        	doNotify(url, listener, urls);
        } catch (Exception t) {
            // 将失败的通知请求记录到失败列表，定时重试
            Map<CoonListener<NURL>, List<NURL>> listeners = failedNotified.get(url);
            if (listeners == null) {
                failedNotified.putIfAbsent(url, new ConcurrentHashMap<CoonListener<NURL>, List<NURL>>());
                listeners = failedNotified.get(url);
            }
            listeners.put(listener, urls);
            logger.error("Failed to notify for subscribe " + url + ", waiting for retry, cause: " + t.getMessage(), t);
        }
    }
    
    protected void doNotify(NURL url, CoonListener<NURL> listener, List<NURL> urls) {
    	super.notify(url, listener, urls);
    }
    
    @Override
    protected void recover() throws Exception {
        // register
        Set<NURL> recoverRegistered = new HashSet<NURL>(getRegistered());
        if (! recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }
            for (NURL url : recoverRegistered) {
                failedRegistered.add(url);
            }
        }
        // subscribe
        Map<NURL, Set<CoonListener<NURL>>> recoverSubscribed = new HashMap<NURL, Set<CoonListener<NURL>>>(getSubscribed());
        if (! recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe url " + recoverSubscribed.keySet());
            }
            for (Map.Entry<NURL, Set<CoonListener<NURL>>> entry : recoverSubscribed.entrySet()) {
            	NURL url = entry.getKey();
                for (CoonListener<NURL> listener : entry.getValue()) {
                    addFailedSubscribed(url, listener);
                }
            }
        }
    }

    // 重试失败的动作
    protected void retry() {
        if (! failedRegistered.isEmpty()) {
            Set<NURL> failed = new HashSet<NURL>(failedRegistered);
            if (failed.size() > 0) {
                if (logger.isInfoEnabled()) {
                    logger.info("Retry register " + failed);
                }
                try {
                    for (NURL url : failed) {
                        try {
                            doRegister(url);
                            failedRegistered.remove(url);
                        } catch (Throwable t) { // 忽略所有异常，等待下次重试
                            logger.warn("Failed to retry register " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                        }
                    }
                } catch (Throwable t) { // 忽略所有异常，等待下次重试
                    logger.warn("Failed to retry register " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                }
            }
        }
        if(! failedUnregistered.isEmpty()) {
            Set<NURL> failed = new HashSet<NURL>(failedUnregistered);
            if (failed.size() > 0) {
                if (logger.isInfoEnabled()) {
                    logger.info("Retry unregister " + failed);
                }
                try {
                    for (NURL url : failed) {
                        try {
                            doUnregister(url);
                            failedUnregistered.remove(url);
                        } catch (Throwable t) { // 忽略所有异常，等待下次重试
                            logger.warn("Failed to retry unregister  " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                        }
                    }
                } catch (Throwable t) { // 忽略所有异常，等待下次重试
                    logger.warn("Failed to retry unregister  " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                }
            }
        }
        if (! failedSubscribed.isEmpty()) {
            Map<NURL, Set<CoonListener<NURL>>> failed = new HashMap<NURL, Set<CoonListener<NURL>>>(failedSubscribed);
            for (Map.Entry<NURL, Set<CoonListener<NURL>>> entry : new HashMap<NURL, Set<CoonListener<NURL>>>(failed).entrySet()) {
                if (entry.getValue() == null || entry.getValue().size() == 0) {
                    failed.remove(entry.getKey());
                }
            }
            if (failed.size() > 0) {
                if (logger.isInfoEnabled()) {
                    logger.info("Retry subscribe " + failed);
                }
                try {
                    for (Map.Entry<NURL, Set<CoonListener<NURL>>> entry : failed.entrySet()) {
                    	NURL url = entry.getKey();
                        Set<CoonListener<NURL>> listeners = entry.getValue();
                        for (CoonListener<NURL> listener : listeners) {
                            try {
                                doSubscribe(url, listener);
                                listeners.remove(listener);
                            } catch (Throwable t) { // 忽略所有异常，等待下次重试
                                logger.warn("Failed to retry subscribe " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                            }
                        }
                    }
                } catch (Throwable t) { // 忽略所有异常，等待下次重试
                    logger.warn("Failed to retry subscribe " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                }
            }
        }
        if (! failedUnsubscribed.isEmpty()) {
            Map<NURL, Set<CoonListener<NURL>>> failed = new HashMap<NURL, Set<CoonListener<NURL>>>(failedUnsubscribed);
            for (Map.Entry<NURL, Set<CoonListener<NURL>>> entry : new HashMap<NURL, Set<CoonListener<NURL>>>(failed).entrySet()) {
                if (entry.getValue() == null || entry.getValue().size() == 0) {
                    failed.remove(entry.getKey());
                }
            }
            if (failed.size() > 0) {
                if (logger.isInfoEnabled()) {
                    logger.info("Retry unsubscribe " + failed);
                }
                try {
                    for (Map.Entry<NURL, Set<CoonListener<NURL>>> entry : failed.entrySet()) {
                    	NURL url = entry.getKey();
                        Set<CoonListener<NURL>> listeners = entry.getValue();
                        for (CoonListener<NURL> listener : listeners) {
                            try {
                                doUnsubscribe(url, listener);
                                listeners.remove(listener);
                            } catch (Throwable t) { // 忽略所有异常，等待下次重试
                                logger.warn("Failed to retry unsubscribe " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                            }
                        }
                    }
                } catch (Throwable t) { // 忽略所有异常，等待下次重试
                    logger.warn("Failed to retry unsubscribe " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                }
            }
        }
        if (! failedNotified.isEmpty()) {
            Map<NURL, Map<CoonListener<NURL>, List<NURL>>> failed = new HashMap<NURL, Map<CoonListener<NURL>, List<NURL>>>(failedNotified);
            for (Map.Entry<NURL, Map<CoonListener<NURL>, List<NURL>>> entry : new HashMap<NURL, Map<CoonListener<NURL>, List<NURL>>>(failed).entrySet()) {
                if (entry.getValue() == null || entry.getValue().size() == 0) {
                    failed.remove(entry.getKey());
                }
            }
            if (failed.size() > 0) {
                if (logger.isInfoEnabled()) {
                    logger.info("Retry notify " + failed);
                }
                try {
                    for (Map<CoonListener<NURL>, List<NURL>> values : failed.values()) {
                        for (Map.Entry<CoonListener<NURL>, List<NURL>> entry : values.entrySet()) {
                            try {
                                CoonListener<NURL> listener = entry.getKey();
                                List<NURL> urls = entry.getValue();
                                listener.notify(urls);
                                values.remove(listener);
                            } catch (Throwable t) { // 忽略所有异常，等待下次重试
                                logger.warn("Failed to retry notify " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                            }
                        }
                    }
                } catch (Throwable t) { // 忽略所有异常，等待下次重试
                    logger.warn("Failed to retry notify " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                }
            }
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            retryFuture.cancel(true);
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    // ==== 模板方法 ====
    protected abstract void doRegister(NURL url);
    protected abstract void doUnregister(NURL url);
    protected abstract void doSubscribe(NURL url, CoonListener<NURL> listener);
    protected abstract void doUnsubscribe(NURL url, CoonListener<NURL> listener);

}