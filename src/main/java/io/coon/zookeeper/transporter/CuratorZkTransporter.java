package io.coon.zookeeper.transporter;

import io.coon.support.Consts;
import io.neural.NURL;
import io.neural.extension.Extension;
import io.neural.micro.ConcurrentHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.api.CuratorWatcher;
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
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Extension("curator")
public class CuratorZkTransporter extends AbstractZkTransporter<CuratorWatcher> {

	private static final Logger logger = LoggerFactory.getLogger(CuratorZkTransporter.class);
	
	private CuratorFramework client;

	@Override
	public void connect(NURL nurl) {
		super.connect(nurl);
		Builder builder = CuratorFrameworkFactory.builder()
				.connectString(nurl.getBackupAddress())
				.retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000))
				.connectionTimeoutMs(nurl.getParameter(Consts.TIMEOUT_KEY, Consts.DEFAULT_REGISTRY_CONNECT_TIMEOUT))
                .sessionTimeoutMs(nurl.getParameter(Consts.SESSION_TIMEOUT_KEY, Consts.DEFAULT_SESSION_TIMEOUT));
		
		String authority = nurl.getAuthority();
		if (authority != null && authority.length() > 0) {
			builder = builder.authorization("digest", authority.getBytes());
		}
		
		client = builder.build();
		client.getConnectionStateListenable().addListener(
			new ConnectionStateListener() {
				public void stateChanged(CuratorFramework client, ConnectionState state) {
					if (state == ConnectionState.LOST) {
						CuratorZkTransporter.this.stateChanged(StateListener.DISCONNECTED);
					} else if (state == ConnectionState.CONNECTED) {
						CuratorZkTransporter.this.stateChanged(StateListener.CONNECTED);
		            	countDownLatch.countDown();
					} else if (state == ConnectionState.RECONNECTED) {
						CuratorZkTransporter.this.stateChanged(StateListener.RECONNECTED);
					}
				}
			});
		client.start();
		
		try {
			countDownLatch.await(nurl.getParameter(Consts.TIMEOUT_KEY, 
					Consts.DEFAULT_REGISTRY_CONNECT_TIMEOUT), TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			logger.error("The countDownLatch exception", e);
		}
	}

	@Override
	public void createPersistent(String path) {
		try {
			client.create().forPath(path);
		} catch (NodeExistsException e) {
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public void createEphemeral(final String path) {
		try {
			client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
		} catch (NodeExistsException e) {
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}
	
	@Override
	public void doCreateData(String path, String json) {
		try {
			client.setData().forPath(path, json.getBytes("UTF-8"));
		} catch (NodeExistsException e) {
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public void delete(String path) {
		try {
			client.delete().forPath(path);
		} catch (NoNodeException e) {
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public List<String> getChildren(String path) {
		try {
			return client.getChildren().forPath(path);
		} catch (NoNodeException e) {
			return new ArrayList<String>();
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public boolean isConnected() {
		return client.getZookeeperClient().isConnected();
	}

	@Override
	public void doClose() {
		client.close();
	}
	
	@Override
	public String doGetChildrenData(String path) {
		try {
			byte[] data = client.getData().forPath(path);
			if(data!=null){
				return new String(data, "UTF-8");
			}
		} catch (NoNodeException e) {
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
		
		return null;
	}

	private class CuratorWatcherImpl implements CuratorWatcher {

		private volatile ChildListener listener;

		public CuratorWatcherImpl(ChildListener listener) {
			this.listener = listener;
		}

		public void unwatch() {
			this.listener = null;
		}

		@Override
		public void process(WatchedEvent event) throws Exception {
			if (listener != null) {
				listener.childChanged(event.getPath(), client.getChildren().usingWatcher(this).forPath(event.getPath()));
			}
		}
	}

	@Override
	public CuratorWatcher createTargetChildListener(String path, ChildListener listener) {
		return new CuratorWatcherImpl(listener);
	}

	@Override
	public List<String> addTargetChildListener(String path, CuratorWatcher listener) {
		try {
			return client.getChildren().usingWatcher(listener).forPath(path);
		} catch (NoNodeException e) {
			return null;
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public void removeTargetChildListener(String path, CuratorWatcher listener) {
		((CuratorWatcherImpl) listener).unwatch();
	}
	
	private final Map<DataListener, PathChildrenCacheListener> dataListenerMap = new ConcurrentHashMap<DataListener, PathChildrenCacheListener>();
	private final Map<String, PathChildrenCache> pathChildrenCacheMap = new ConcurrentHashMap<String, PathChildrenCache>();
	private final Map<String, Set<DataListener>> dataListenersMap = new ConcurrentHashMap<String, Set<DataListener>>();
	private final Map<String, Map<String, String>> childDataMap = new ConcurrentHashMap<String, Map<String, String>>();
	
	@Override
	public void addDataListener(String path, DataListener listener) {
		try {
			// 第一步：获取-校验-创建监听器
			PathChildrenCacheListener pathChildrenCacheListener = dataListenerMap.get(listener);
			if(pathChildrenCacheListener != null){//已监听
				return;
			} else {
				// 添加外部监听器
				Set<DataListener> dataListenerSet = dataListenersMap.get(path);
				if(dataListenerSet == null){
					dataListenersMap.put(path, dataListenerSet = new ConcurrentHashSet<DataListener>());
				}
				dataListenerSet.add(listener);
				dataListenerMap.put(listener, pathChildrenCacheListener = new PathChildrenCacheListenerImpl(path));
			}
			
			// 第二步：获取-校验-创建子节点缓存连接
			PathChildrenCache pathChildrenCache = pathChildrenCacheMap.get(path);
			if(pathChildrenCache == null){
				pathChildrenCacheMap.put(path, pathChildrenCache = new PathChildrenCache(client, path, true));
				// 第三步：启动监听
				pathChildrenCache.start(StartMode.POST_INITIALIZED_EVENT);
			}
			
			// 第四步：添加监听器
	        pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}
	
	@Override
	public void removeDataListener(String path, DataListener listener) {
		try {
			// 第一步：移除dataListenerMap中的数据
			PathChildrenCacheListener pathChildrenCacheListener = dataListenerMap.get(listener);
			if(pathChildrenCacheListener == null){
				return;
			} else {
				dataListenerMap.remove(listener);
				
				// 第二步：移除Set<DataListener>中的数据
				Set<DataListener> dataListenerSet = dataListenersMap.get(path);
				if(dataListenerSet != null && dataListenerSet.contains(listener)){
					dataListenerSet.remove(listener);
				}

				// 第三步：移除dataListenersMap和childDataMap中的数据
				if(dataListenerSet == null || dataListenerSet.isEmpty()){
					dataListenersMap.remove(path);
					childDataMap.remove(path);
				}
			}
			
			// 第四步：取消监听,并移除pathChildrenCacheMap中的数据
			PathChildrenCache pathChildrenCache = pathChildrenCacheMap.get(path);
			if(pathChildrenCache != null){
				pathChildrenCache.getListenable().removeListener(pathChildrenCacheListener);
				((PathChildrenCacheListenerImpl)listener).unwatch();
				if(pathChildrenCache.getListenable().size() == 0){
					pathChildrenCacheMap.remove(path);
					pathChildrenCache.close();
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	private class PathChildrenCacheListenerImpl implements PathChildrenCacheListener {
		
		private volatile String path;
		private volatile Set<DataListener> dataListenerSet;
		private volatile Map<String, String> childrenDataMap;
		private volatile boolean completeInit = false;
		
		public PathChildrenCacheListenerImpl(String path) {
			this.path = path;
			this.dataListenerSet =  dataListenersMap.get(path);
			this.childrenDataMap = childDataMap.get(path);
			if(childrenDataMap == null){
				childDataMap.put(path, childrenDataMap = new ConcurrentHashMap<String, String>());
			}
		}

		public void unwatch() {
			this.path = null;
		}

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			if (path != null) {
				if(event.getInitialData() != null){//判断当前是不是初始化通知
					completeInit = true;
					List<ChildData> childDatas = event.getInitialData();
					if(childDatas != null && childDatas.size() > 0){
						for (ChildData childData:childDatas) {
							childrenDataMap.put(childData.getPath(), new String(childData.getData(), "UTF-8"));
						}
						this.doNotify();// 订阅后初始化成功,则进行第一次广播
					}
				} else {
					if(!completeInit){// 没有初始化成功前,不进行变更通知操作
						return;
					}
					ChildData childData = event.getData();
					if(childData!=null){
						String data = new String(childData.getData(), "UTF-8");
						logger.debug(String.format("ZK-Curator Path[%s] change[EventType: %s] data[%s]", this.path, event.getType(), data));
						
						switch (event.getType()) {  
		                case CHILD_REMOVED:
		                	childrenDataMap.remove(childData.getPath());
		                	this.doNotify();// 数据删除,开始广播最新列表
		                    break;
		                case CHILD_ADDED:// Added动作通知IP地址,而非配置数据
		                	childrenDataMap.remove(childData.getPath());
		                    break;
		                case CHILD_UPDATED:
		                	childrenDataMap.put(childData.getPath(), data);
		                	this.doNotify();// 数据更新,开始广播最新列表
		                    break;
		                default:
		                	logger.debug("ZK-Curator is unknown action: {}", event.getType().toString());
		                    break;  
		                }
					}
				}
			}
		}
		
		/**
		 * 串联向外广播最新列表
		 */
		private void doNotify(){
			if(dataListenerSet != null && dataListenerSet.size() > 0) {
				for (DataListener dataListener: dataListenerSet) {
					dataListener.dataChanged(path, childrenDataMap);
				}						
			}
		}
	}
	
}
