package cn.ms.coon.zookeeper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.CoonListener;
import cn.ms.coon.support.AbstractMconf;
import cn.ms.coon.support.common.Mcf;
import cn.ms.neural.NURL;
import cn.ms.neural.util.micro.ConcurrentHashSet;

import com.alibaba.fastjson.JSON;

/**
 * The base of Zookeeper Mconf.
 * 
 * @author lry
 */
public class ZookeeperMconf extends AbstractMconf {

	private static final Logger logger = LoggerFactory.getLogger(ZookeeperMconf.class);

	private CuratorFramework client;
	private ConnectionState globalState = null;

	private final ExecutorService pool = Executors.newFixedThreadPool(2);
	@SuppressWarnings("rawtypes")
	private final Map<String, Set<CoonListener>> pushNotifyMap = new ConcurrentHashMap<String, Set<CoonListener>>();
	private final Map<String, Map<String, Object>> pushMap = new ConcurrentHashMap<String, Map<String, Object>>();
	private final Map<String, PathChildrenCache> pathChildrenCacheMap = new ConcurrentHashMap<String, PathChildrenCache>();

	@Override
	public void connect(NURL url) {
		super.connect(url);

		String connAddrs = url.getBackupAddress();
		// Connection timeout, defaults to 60s
		int timeout = url.getParameter("timeout", 60 * 1000);
		// Expired cleanup time, defaults to 60s
		int session = url.getParameter("session", 60 * 1000);

		Builder builder = CuratorFrameworkFactory.builder().connectString(connAddrs)
				.retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000))
				.connectionTimeoutMs(timeout).sessionTimeoutMs(session);
		final CountDownLatch cd = new CountDownLatch(1);
		client = builder.build();
		client.getConnectionStateListenable().addListener(
				new ConnectionStateListener() {
					public void stateChanged(CuratorFramework client, ConnectionState state) {
						logger.info("The registration center connection status is changed to [{}].", state);
						if (globalState == null || state == ConnectionState.CONNECTED) {
							cd.countDown();
							globalState = state;
						}
					}
				});
		client.start();

		try {
			cd.await(timeout, TimeUnit.MILLISECONDS);
			if (ConnectionState.CONNECTED != globalState) {
				throw new TimeoutException("The connection zookeeper is timeout.");
			}
		} catch (Exception e) {
			logger.error("The await exception.", e);
		}
	}

	@Override
	public boolean available() {
		return client.getZookeeperClient().isConnected();
	}

	@Override
	public void addConf(Mcf mcf, Object obj) {
		String path = mcf.buildRoot(super.url).getKey();
		
		byte[] dataByte = null;
		try {
			String json = super.obj2Json(obj);
			logger.debug("The PATH[{}] add conf data[{}].", path, json);

			dataByte = json.getBytes(Charset.forName("UTF-8"));
		} catch (Exception e) {
			throw new IllegalStateException("Serialized data exception.", e);
		}

		try {
			client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, dataByte);
		} catch (NodeExistsException e) {
		} catch (Exception e) {
			throw new IllegalStateException("Add data exception.", e);
		}
	}

	@Override
	public void delConf(Mcf mcf) {
		try {
			String path;
			if (AbstractMconf.isNotBlank(mcf.getData())) {
				path = mcf.buildRoot(super.url).getKey();
				logger.debug("The PATH[{}] delete conf data.", path);
			} else {
				path = mcf.buildRoot(super.url).getPrefixKey();
				logger.debug("The PATH[{}] and SubPATH delete conf datas.", path);
			}
			
			client.delete().deletingChildrenIfNeeded().forPath(path);
		} catch (NoNodeException e) {
		} catch (Exception e) {
			throw new IllegalStateException("Delete data exception.", e);
		}
	}

	@Override
	public void upConf(Mcf mcf, Object obj) {
		String path = mcf.buildRoot(super.url).getKey();
		
		byte[] dataByte = null;
		try {
			String json = super.obj2Json(obj);
			logger.debug("The PATH[{}] update conf data[{}].", path, json);
			
			dataByte = json.getBytes(Charset.forName("UTF-8"));
		} catch (Exception e) {
			throw new IllegalStateException("Serialized data exception.", e);
		}

		try {
			client.setData().forPath(path, dataByte);
		} catch (NoNodeException e) {
		} catch (Exception e) {
			throw new IllegalStateException("UpConf data exception.", e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T pull(Mcf mcf, Class<T> cls) {
		String path = mcf.buildRoot(super.url).getKey();
		logger.debug("The PATH[{}] pull conf data.", path);
		
		byte[] dataByte = null;
		try {
			dataByte = client.getData().forPath(path);
		} catch (NoNodeException e) {
		} catch (Exception e) {
			throw new IllegalStateException("Pull data exception.", e);
		}

		if (dataByte == null) {
			return null;
		} else {
			String json = new String(dataByte, Charset.forName("UTF-8"));
			logger.debug("The PATH[{}] pulled conf data[{}].", path, json);
			
			try {
				if (cls == null) {
					return (T)json;
				} else {
					return super.json2Obj(json, cls);
				}
			} catch (Exception e) {
				throw new IllegalStateException("UnSerialized data exception.", e);
			}
		}
	}

	@Override
	public <T> List<T> pulls(Mcf mcf, Class<T> cls) {
		String path = mcf.buildRoot(super.url).getPrefixKey();
		logger.debug("The PATH[{}] pulls conf data.", path);
		
		// Query all dataId lists
		List<T> list = new ArrayList<T>();
		List<String> childNodeList = null;
		try {
			childNodeList = client.getChildren().forPath(path);
		} catch (NoNodeException e) {
		} catch (Exception e) {
			throw new IllegalStateException("Gets all child node exceptions.", e);
		}

		if (childNodeList == null) {
			return list;
		}
		
		for (String childNode : childNodeList) {
			String json;
			byte[] dataByte = null;
			String allPath = path + "/" + childNode;
			
			try {
				dataByte = client.getData().forPath(allPath);
			} catch (NoNodeException e) {
			} catch (Exception e) {
				throw new IllegalStateException("Modify data exception.", e);
			}

			if (dataByte == null) {
				continue;
			}

			try {
				json = new String(dataByte, Charset.forName("UTF-8"));
				logger.debug("The PATH[{}] pullsed conf data[{}].", allPath, json);
			} catch (Exception e) {
				throw new IllegalStateException("UnSerialized data exception.", e);
			}
			
			if (AbstractMconf.isBlank(json)) {
				continue;
			} else {
				T t = super.json2Obj(json, cls);
				if (t != null) {
					list.add(t);
				}				
			}
		}

		return list;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public <T> void push(final Mcf mcf, final Class<T> cls, final CoonListener<T> notify) {
		final String path = mcf.buildRoot(super.url).getPrefixKey();
		if (AbstractMconf.isBlank(path)) {
			throw new RuntimeException("The PATH cannot be empty, path==" + path);
		}

		// 允许多个监听者监听同一个节点
		Set<CoonListener> notifies = pushNotifyMap.get(path);
		if (notifies == null) {
			pushNotifyMap.put(path, notifies = new ConcurrentHashSet<CoonListener>());
		}
		notifies.add(notify);

		if (pushMap.containsKey(path)) {// 已被订阅
			List list = new ArrayList();
			list.addAll(pushMap.get(path).values());
			notify.notify(list);// 通知一次
		} else {
			final Map<String, Object> tempMap;
			pushMap.put(path, tempMap = new ConcurrentHashMap<String, Object>());

			try {
				final PathChildrenCache childrenCache = new PathChildrenCache(client, path, true);
				childrenCache.start(StartMode.POST_INITIALIZED_EVENT);
				childrenCache.getListenable().addListener(
						new PathChildrenCacheListener() {
							private boolean isInit = false;

							@Override
							public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
								ChildData childData = event.getData();
								if (event.getInitialData() != null) {
									isInit = true;
								}

								if (childData == null) {
									logger.debug("The is listenering PATH[{}], initialization notify all data[{}].", path, JSON.toJSONString(tempMap));
								} else {
									String tempPath = event.getData().getPath();
									String tempJsonData = new String(event.getData().getData(), Charset.forName("UTF-8"));
									T t = (T) JSON.parseObject(tempJsonData, cls);

									if (PathChildrenCacheEvent.Type.CHILD_ADDED == event.getType()
											|| PathChildrenCacheEvent.Type.CHILD_UPDATED == event.getType()) {
										tempMap.put(tempPath, t);
									} else if (PathChildrenCacheEvent.Type.CHILD_REMOVED == event.getType()) {
										tempMap.remove(tempPath);
									}

									if (isInit) {
										logger.debug("The changed PATH[{}] update data[{}].", tempPath, tempJsonData);
										logger.debug("The changed PATH[{}] notify all datas[{}].", path, JSON.toJSONString(tempMap));
										Set<CoonListener> tempNotifySet = pushNotifyMap.get(path);
										for (CoonListener tempNotify : tempNotifySet) {// 通知每一个监听器
											List list = new ArrayList();
											list.addAll(tempMap.values());
											tempNotify.notify(list);
										}
									}
								}
							}
						}, pool);
				pathChildrenCacheMap.put(path, childrenCache);
			} catch (Exception e) {
				logger.error("The PathChildrenCache add listener exception.", e);
			}
		}
	}

	@Override
	public void unpush(Mcf mcf) {
		String path = mcf.buildRoot(super.url).getPrefixKey();
		if (AbstractMconf.isBlank(path)) {
			throw new RuntimeException("The PATH cannot be empty, path==" + path);
		}

		PathChildrenCache pathChildrenCache = pathChildrenCacheMap.get(path);
		if (pathChildrenCache != null) {
			try {
				pathChildrenCache.close();
			} catch (IOException e) {
				logger.error("PathChildrenCache close exception.", e);
			}
		}

		if (pushNotifyMap.containsKey(path)) {
			pushNotifyMap.remove(path);
		}

		if (pushMap.containsKey(path)) {
			pushMap.remove(path);
		}
	}
	
	@Override
	public void destroy() {
		// TODO Auto-generated method stub
	}

}
