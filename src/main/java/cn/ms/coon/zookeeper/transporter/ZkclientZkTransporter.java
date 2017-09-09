package cn.ms.coon.zookeeper.transporter;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import cn.ms.coon.support.Consts;
import cn.ms.neural.NURL;
import cn.ms.neural.extension.Extension;
import cn.ms.neural.util.micro.ConcurrentHashSet;

@Extension("zkclient")
public class ZkclientZkTransporter extends AbstractZkTransporter<IZkChildListener> {

	private ZkClient client;
	private volatile KeeperState state = KeeperState.SyncConnected;

	@Override
	public void connect(NURL url) {
		super.connect(url);
		client = new ZkClient(
                url.getBackupAddress(),
                url.getParameter(Consts.SESSION_TIMEOUT_KEY, Consts.DEFAULT_SESSION_TIMEOUT),
                url.getParameter(Consts.TIMEOUT_KEY, Consts.DEFAULT_REGISTRY_CONNECT_TIMEOUT),
                new SerializableSerializer());
		
		client.subscribeStateChanges(new IZkStateListener() {
			@Override
			public void handleStateChanged(KeeperState state) throws Exception {
				ZkclientZkTransporter.this.state = state;
				if (state == KeeperState.Disconnected) {
					stateChanged(StateListener.DISCONNECTED);
				} else if (state == KeeperState.SyncConnected) {
					stateChanged(StateListener.CONNECTED);
				}
			}
			
			@Override
			public void handleNewSession() throws Exception {
				stateChanged(StateListener.RECONNECTED);
			}
			
			@Override
			public void handleSessionEstablishmentError(Throwable error) throws Exception {
			}
		});
	}

	@Override
	public void createPersistent(String path) {
		try {
			client.createPersistent(path, true);
		} catch (ZkNodeExistsException e) {
		}
	}
	
	@Override
	public void createEphemeral(String path) {
		try {
			client.createEphemeral(path);
		} catch (ZkNodeExistsException e) {
		}
	}
	
	@Override
	public void doCreateData(String path, byte[] data) {
		try {
			client.writeData(path, data);
		} catch (ZkNodeExistsException e) {
		}
	}

	@Override
	public void delete(String path) {
		try {
			client.delete(path);
		} catch (ZkNoNodeException e) {
		}
	}

	@Override
	public List<String> getChildren(String path) {
		try {
			return client.getChildren(path);
        } catch (ZkNoNodeException e) {
            return null;
        }
	}

	@Override
	public boolean isConnected() {
		return state == KeeperState.SyncConnected;
	}

	@Override
	public void doClose() {
		client.close();
	}

	@Override
	public IZkChildListener createTargetChildListener(String path, final ChildListener listener) {
		return new IZkChildListener() {
			@Override
			public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
				listener.childChanged(parentPath, currentChilds);
			}
		};
	}

	@Override
	public List<String> addTargetChildListener(String path, final IZkChildListener listener) {
		return client.subscribeChildChanges(path, listener);
	}

	@Override
	public void removeTargetChildListener(String path, IZkChildListener listener) {
		client.unsubscribeChildChanges(path,  listener);
	}
	
	private final Map<DataListener, IZkDataListener> dataListenerMap = new ConcurrentHashMap<DataListener, IZkDataListener>();
	private final Map<String, Set<DataListener>> dataListenersMap = new ConcurrentHashMap<String, Set<DataListener>>();
	private final Map<String, Map<String, byte[]>> childDataMap = new ConcurrentHashMap<String, Map<String, byte[]>>();
	
	@Override
	public void addDataListener(String path, DataListener listener) {
		try {
			// 第一步：获取-校验-创建监听器
			IZkDataListener iZkDataListener = dataListenerMap.get(listener);
			if(iZkDataListener != null){//已监听
				return;
			} else {
				// 添加外部监听器
				Set<DataListener> dataListenerSet = dataListenersMap.get(path);
				if(dataListenerSet == null){
					dataListenersMap.put(path, dataListenerSet = new ConcurrentHashSet<DataListener>());
				}
				dataListenerSet.add(listener);
				dataListenerMap.put(listener, iZkDataListener = new IZkDataListenerImpl(path));
			}
			
			// 第二步：启动监听
			client.subscribeDataChanges(path, new IZkDataListener() {  
	            @Override  
	            public void handleDataChange(String s, Object o) throws Exception {  
	                System.out.println("变更节点为：" + s + "，变更数据为：" + o);  
	            }  
	  
	            @Override  
	            public void handleDataDeleted(String s) throws Exception {  
	                System.out.println("删除的节点为：" + s);  
	            }  
	        });
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}
	
	@Override
	public void removeDataListener(String path, DataListener listener) {
		try {
			// 第一步：移除dataListenerMap中的数据
			IZkDataListener iZkDataListener = dataListenerMap.get(listener);
			if(iZkDataListener == null){
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
			
			// 第四步：取消监听
			client.unsubscribeDataChanges(path, iZkDataListener);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	private class IZkDataListenerImpl implements IZkDataListener {
		
		private volatile String path;
		private volatile Set<DataListener> dataListenerSet;
		private volatile Map<String, byte[]> childrenDataMap;
		
		public IZkDataListenerImpl(String path) {
			this.path = path;
			this.dataListenerSet =  dataListenersMap.get(path);
			this.childrenDataMap = childDataMap.get(path);
			if(childrenDataMap == null){
				childDataMap.put(path, childrenDataMap = new ConcurrentHashMap<String, byte[]>());
			}
		}
		
		@Override
		public void handleDataChange(String dataPath, Object data) throws Exception {
			System.out.println("变更节点为：" + dataPath + "，变更数据为：" + data);  
			childrenDataMap.put(dataPath, (byte[])data);
			this.doNotify();
		}

		@Override
		public void handleDataDeleted(String dataPath) throws Exception {
			System.out.println("删除的节点为：" +dataPath);  
			childrenDataMap.remove(dataPath);
			this.doNotify();
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
