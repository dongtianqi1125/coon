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
import cn.ms.coon.support.CoonListener;
import cn.ms.coon.support.NamedThreadFactory;
import cn.ms.coon.support.mconf.AbstractMconf;
import cn.ms.coon.support.mconf.Mcf;
import cn.ms.neural.NURL;
import cn.ms.neural.extension.Extension;
import cn.ms.neural.util.BeanUtils;
import cn.ms.neural.util.micro.ConcurrentHashSet;

/**
 * The base of Redis Mconf.
 * 
 * @author lry
 */
@Extension("redis")
public class RedisMconf extends AbstractMconf {

	private static final Logger logger = LoggerFactory.getLogger(RedisMconf.class);

	private JedisPool jedisPool;
	private long retryPeriod = 10000;
	private boolean isSubscribe = true;

	private final Map<String, Class<?>> pushClassMap = new ConcurrentHashMap<String, Class<?>>();
	@SuppressWarnings("rawtypes")
	private final ConcurrentMap<String, Set<CoonListener>> pushNotifyMap = new ConcurrentHashMap<String, Set<CoonListener>>();
	private final ConcurrentMap<String, Map<String, String>> pushValueMap = new ConcurrentHashMap<String, Map<String, String>>();
	@SuppressWarnings("unused")
	private ScheduledFuture<?> retryFuture;
	private final ScheduledExecutorService retryExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("RedisMconfTimer", true));

	@Override
	public void connect(NURL nurl) {
		super.connect(nurl);
		this.retryPeriod = nurl.getParameter("retryPeriod", retryPeriod);

		JedisPoolConfig config = new JedisPoolConfig();
		Map<String, String> parameters = nurl.getParameters();
		if (parameters != null) {
			if (!parameters.isEmpty()) {
				try {
					BeanUtils.copyProperties(config, nurl.getParameters());
				} catch (Exception e) {
					logger.error("The copy properties exception.", e);
				}
			}
		}

		jedisPool = new JedisPool(config, nurl.getHost(), nurl.getPort());
	}

	@Override
	public boolean available() {
		return (jedisPool == null) ? false : (!jedisPool.isClosed());
	}

	@Override
	public void publish(Mcf mcf, Object obj) {
		String key = mcf.buildRoot(super.nurl).getPrefixKey();
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
	public void unpublish(Mcf mcf, Object obj) {
		String key = mcf.buildRoot(super.nurl).getPrefixKey();
		Jedis jedis = null;
		
		try {
			jedis = jedisPool.getResource();
			String field = mcf.getSuffixKey();
			if (super.isNotBlank(field)) {
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

	@SuppressWarnings("rawtypes")
	@Override
	public <T> void subscribe(Mcf mcf, Class<T> cls, CoonListener<T> listener) {
		if (isSubscribe) {
			this.pushSubscribe();
			isSubscribe = false;
		}

		String key = mcf.buildRoot(super.nurl).getPrefixKey();
		Jedis jedis = null;
		
		try {
			jedis = jedisPool.getResource();
			if (!pushClassMap.containsKey(key)) {
				pushClassMap.put(key, cls);
			}

			Set<CoonListener> notifies = pushNotifyMap.get(key);
			if (notifies == null) {
				pushNotifyMap.put(key, notifies = new ConcurrentHashSet<CoonListener>());
			}
			notifies.add(listener);

			// 第一次拉取式通知
			Map<String, String> dataMap = jedis.hgetAll(key);
			if (dataMap == null) {
				dataMap = new HashMap<String, String>();
			}

			List<T> list = new ArrayList<T>();
			for (String tempJson : dataMap.values()) {
				list.add(super.json2Obj(tempJson, cls));
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
	public <T> void unsubscribe(Mcf mcf, CoonListener<T> listener) {
		String key = mcf.buildRoot(super.nurl).getPrefixKey();
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

	@Override
	public <T> T lookup(Mcf mcf, Class<T> cls) {
		String key = mcf.buildRoot(super.nurl).getPrefixKey();
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
	public <T> List<T> lookups(Mcf mcf, Class<T> cls) {
		String key = mcf.buildRoot(super.nurl).getPrefixKey();
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Map<String, String> dataMap = jedis.hgetAll(key);
			List<T> list = new ArrayList<T>();
			for (String tempJson : dataMap.values()) {
				list.add(super.json2Obj(tempJson, cls));
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
	
	@Override
	public Map<String, Map<String, String>> apps() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Map<String, Map<String, String>> confs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void destroy() {
		if (retryExecutor != null) {
			retryExecutor.shutdown();
		}
		if (jedisPool != null) {
			jedisPool.destroy();
		}
		if (jedisPool != null) {
			jedisPool.close();
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
								Set<CoonListener> notifies = pushNotifyMap.get(entry.getKey());
								if (notifies == null) {
									continue;
								} else {
									pushValueMap.put(entry.getKey(), newMap);
									for (CoonListener notify : notifies) {
										List list = new ArrayList();
										for (Map.Entry<String, String> tempEntry : newMap.entrySet()) {
											list.add(json2Obj(tempEntry.getValue(), entry.getValue()));
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

}
