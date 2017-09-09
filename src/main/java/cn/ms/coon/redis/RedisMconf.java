package cn.ms.coon.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import cn.ms.coon.ServiceListener;
import cn.ms.coon.support.AbstractMconf;
import cn.ms.coon.support.common.Mcf;
import cn.ms.coon.support.common.NamedThreadFactory;
import cn.ms.neural.NURL;
import cn.ms.neural.util.BeanUtils;
import cn.ms.neural.util.micro.ConcurrentHashSet;

import com.alibaba.fastjson.JSON;

/**
 * The base of Redis Mconf.
 * 
 * @author lry
 */
public class RedisMconf extends AbstractMconf {

	private static final Logger logger = LoggerFactory.getLogger(RedisMconf.class);

	private JedisPool jedisPool;
	private long retryPeriod = 10000;
	private boolean isSubscribe = true;

	private final Map<String, Class<?>> pushClassMap = new ConcurrentHashMap<String, Class<?>>();
	@SuppressWarnings("rawtypes")
	private final ConcurrentMap<String, Set<ServiceListener>> pushNotifyMap = new ConcurrentHashMap<String, Set<ServiceListener>>();
	private final ConcurrentMap<String, Map<String, String>> pushValueMap = new ConcurrentHashMap<String, Map<String, String>>();

	@SuppressWarnings("unused")
	private ScheduledFuture<?> retryFuture;
	private final ScheduledExecutorService retryExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("RedisMconfTimer", true));

	@Override
	public void connect(NURL url) {
		super.connect(url);
		this.retryPeriod = url.getParameter("retryPeriod", retryPeriod);

		JedisPoolConfig config = new JedisPoolConfig();
		Map<String, String> parameters = url.getParameters();
		if (parameters != null) {
			if (!parameters.isEmpty()) {
				try {
					BeanUtils.copyProperties(config, url.getParameters());
				} catch (Exception e) {
					logger.error("The copy properties exception.", e);
				}
			}
		}

		jedisPool = new JedisPool(config, url.getHost(), url.getPort());
	}

	@Override
	public boolean available() {
		return (jedisPool == null) ? false : (!jedisPool.isClosed());
	}

	@Override
	public void addConf(Mcf mcf, Object obj) {
		String key = mcf.buildRoot(super.url).getPrefixKey();
		String field = mcf.getSuffixKey();
		String json = this.obj2Json(obj);
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			jedis.hset(key, field, json);
		} catch (Exception e) {
			logger.error("The add conf exception.", e);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public void delConf(Mcf mcf) {
		String key = mcf.buildRoot(super.url).getPrefixKey();
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			String field = mcf.getSuffixKey();
			if (AbstractMconf.isNotBlank(field)) {
				jedis.hdel(key, field);
			} else {
				jedis.hdel(key);
			}
		} catch (Exception e) {
			logger.error("The delete conf exception.", e);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public void upConf(Mcf mcf, Object obj) {
		this.addConf(mcf, obj);
	}

	@Override
	public <T> T pull(Mcf mcf, Class<T> cls) {
		String key = mcf.buildRoot(super.url).getPrefixKey();
		String field = mcf.getSuffixKey();
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			String json = jedis.hget(key, field);
			return (T) json2Obj(json, cls);
		} catch (Exception e) {
			logger.error("The pull conf exception.", e);
			return null;
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public <T> List<T> pulls(Mcf mcf, Class<T> cls) {
		String key = mcf.buildRoot(super.url).getPrefixKey();
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Map<String, String> dataMap = jedis.hgetAll(key);
			List<T> list = new ArrayList<T>();
			for (String tempJson:dataMap.values()) {
				list.add(JSON.parseObject(tempJson, cls));
			}
			
			return list;
		} catch (Exception e) {
			logger.error("The pulls conf exception.", e);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}

		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public <T> void push(Mcf mcf, Class<T> cls, ServiceListener<T> listener) {
		if (isSubscribe) {
			this.pushSubscribe();
			isSubscribe = false;
		}
		
		String key = mcf.buildRoot(super.url).getPrefixKey();
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			if (!pushClassMap.containsKey(key)) {
				pushClassMap.put(key, cls);
			}

			Set<ServiceListener> notifies = pushNotifyMap.get(key);
			if (notifies == null) {
				pushNotifyMap.put(key, notifies = new ConcurrentHashSet<ServiceListener>());
			}
			notifies.add(listener);

			// 第一次拉取式通知
			Map<String, String> dataMap = jedis.hgetAll(key);
			if (dataMap == null) {
				dataMap = new HashMap<String, String>();
			}

			List<T> list = new ArrayList<T>();
			for (String tempJson:dataMap.values()) {
				list.add(JSON.parseObject(tempJson, cls));
			}
			
			pushValueMap.put(key, dataMap);
			listener.notify(list);
		} catch (Exception e) {
			logger.error("The push conf exception.", e);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public void unpush(Mcf mcf) {
		String key = mcf.buildRoot(super.url).getPrefixKey();
		if (pushClassMap.containsKey(key)) {
			pushClassMap.remove(key);
		}
		if (pushNotifyMap.containsKey(key)) {
			pushNotifyMap.remove(key);
		}
		if (pushValueMap.containsKey(key)) {
			pushValueMap.remove(key);
		}
	}

	/**
	 * 定时拉取数据
	 */
	private void pushSubscribe() {
		if (!isSubscribe) {
			return;
		}

		this.retryFuture = retryExecutor.scheduleWithFixedDelay(new Runnable() {
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public void run() {
				try {
					if (pushClassMap.isEmpty()) {
						return;
					}

					for (Map.Entry<String, Class<?>> entry : pushClassMap.entrySet()) {
						Jedis jedis = null;
						try {
							jedis = jedisPool.getResource();
							Map<String, String> newMap = jedis.hgetAll(entry.getKey());
							if (newMap == null) {
								newMap = new HashMap<String, String>();
							}
							Map<String, String> oldMap = pushValueMap.get(entry.getKey());
							if (!newMap.equals(oldMap)) {// 已变更
								Set<ServiceListener> notifies = pushNotifyMap.get(entry.getKey());
								if (notifies == null) {
									continue;
								} else {
									pushValueMap.put(entry.getKey(), newMap);
									for (ServiceListener notify : notifies) {
										List list = new ArrayList();
										for (Map.Entry<String, String> tempEntry : newMap.entrySet()) {
											list.add(JSON.parseObject(tempEntry.getValue(), entry.getValue()));
										}

										notify.notify(list);
									}
								}
							}
						} catch (Exception e) {
							logger.error("The push conf exception.", e);
						} finally {
							if (jedis != null) {
								jedis.close();
							}
						}
					}
				} catch (Exception e) { // 防御性容错
					logger.error("Unexpected error occur at failed retry, cause: " + e.getMessage(), e);
				}
			}
		}, retryPeriod, retryPeriod, TimeUnit.MILLISECONDS);
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
	}
	
}
