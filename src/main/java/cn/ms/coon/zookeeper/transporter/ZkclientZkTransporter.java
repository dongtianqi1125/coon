package cn.ms.coon.zookeeper.transporter;

import io.neural.NURL;
import io.neural.extension.Extension;
import io.neural.util.micro.ConcurrentHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.support.Consts;

@Extension("zkclient")
public class ZkclientZkTransporter extends AbstractZkTransporter<IZkChildListener> {

	private static final Logger logger = LoggerFactory.getLogger(ZkclientZkTransporter.class);
	
	private ZkClient client;
	private volatile KeeperState state = KeeperState.SyncConnected;

	@Override
	public void connect(NURL nurl) {
		super.connect(nurl);
		client = new ZkClient(
				nurl.getBackupAddress(),
				nurl.getParameter(Consts.SESSION_TIMEOUT_KEY, Consts.DEFAULT_SESSION_TIMEOUT),
				nurl.getParameter(Consts.TIMEOUT_KEY, Consts.DEFAULT_REGISTRY_CONNECT_TIMEOUT));
		
		client.subscribeStateChanges(new IZkStateListener() {
			@Override
			public void handleStateChanged(KeeperState state) throws Exception {
				ZkclientZkTransporter.this.state = state;
				if (state == KeeperState.Disconnected) {
					stateChanged(StateListener.DISCONNECTED);
				} else if (state == KeeperState.SyncConnected) {
					stateChanged(StateListener.CONNECTED);
					countDownLatch.countDown();
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
	public void doCreateData(String path, String json) {
		try {
			client.writeData(path, json);
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
            return new ArrayList<String>();
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
	public String doGetChildrenData(String path) {
		try {
			return client.readData(path);
        } catch (ZkNoNodeException e) {
            return null;
        }
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
	private final Map<String, Map<String, String>> childDataMap = new ConcurrentHashMap<String, Map<String, String>>();
	
	private final Map<String, IZkChildListener> iZkChildListenerMap = new ConcurrentHashMap<String, IZkChildListener>();
	private final Map<String, Set<String>> memoryChildrenMap = new ConcurrentHashMap<String, Set<String>>();
	
	@Override
	public void addDataListener(final String path, final DataListener listener) {
		IZkChildListener iZkChildListener = iZkChildListenerMap.get(path);
		if(iZkChildListener != null){
			return;
		} else {
			iZkChildListenerMap.put(path, iZkChildListener = new IZkChildListener() {
				@Override
				public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
					if(currentChilds != null){
						for (int i = 0; i < currentChilds.size(); i++) {
							currentChilds.set(i, path + "/" + currentChilds.get(i));
						}
						System.out.println("1最新子节点列表："+currentChilds);
						doSubscribeChildrens(path, currentChilds);
					}
				}
			});
			
			// 订阅当前节点下的所有子节点
			client.subscribeChildChanges(path, iZkChildListener);
			// 订阅后通过查找的方式来完成第一次广播动作
			List<String> childrens = client.getChildren(path);
			for (int i = 0; i < childrens.size(); i++) {
				childrens.set(i, path + "/" + childrens.get(i));
			}
			System.out.println("2最新子节点列表：" + childrens);
			this.doSubscribeChildrens(path, childrens);
			
		}
	}
	
	private void doSubscribeChildrens(String path, List<String> childrens) {
		Set<String> tempNewChildrenSet = new ConcurrentHashSet<String>();
		if(childrens!=null){
			if(!childrens.isEmpty()){
				tempNewChildrenSet.addAll(childrens);
			}
		}
		System.out.println("1---->"+tempNewChildrenSet);
		
		Set<String> memoryChildrenSet = memoryChildrenMap.get(path);
		if(memoryChildrenSet == null){
			memoryChildrenMap.put(path, memoryChildrenSet = new ConcurrentHashSet<String>());
		}
		
		Set<String> tempMemoryChildrenSet = new ConcurrentHashSet<String>();
		if(!memoryChildrenSet.isEmpty()){
			tempMemoryChildrenSet.addAll(memoryChildrenSet);
		}
		System.out.println("2---->"+tempMemoryChildrenSet);
		
		// 需要订阅的子节点=最新节点列表-内存节点列表
		if(!tempMemoryChildrenSet.isEmpty()){
			tempNewChildrenSet.removeAll(tempMemoryChildrenSet);
		}
		System.out.println("3---->"+tempNewChildrenSet);
		
		// 需要取消订阅的子节点=内存节点列表-最新节点列表
		if(childrens!=null){
			if(!childrens.isEmpty()){
				tempMemoryChildrenSet.removeAll(childrens);
			}
		}
		System.out.println("4---->"+tempMemoryChildrenSet);
	}
	
	@Override
	public void removeDataListener(String path, DataListener listener) {
		
	}
	
	public void doAddDataListener(String path, DataListener listener) {
		try {
			// 第一步：获取-校验-创建监听器
			IZkDataListener iZkDataListener = dataListenerMap.get(listener);
			if(iZkDataListener != null){// 已监听
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
			client.subscribeDataChanges(path, iZkDataListener);
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}
	
	public void doRemoveDataListener(String path, DataListener listener) {
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
		private volatile Map<String, String> childrenDataMap;
		
		public IZkDataListenerImpl(String path) {
			this.path = path;
			this.dataListenerSet =  dataListenersMap.get(path);
			this.childrenDataMap = childDataMap.get(path);
			if(childrenDataMap == null){
				childDataMap.put(path, childrenDataMap = new ConcurrentHashMap<String, String>());
			}
		}
		
		@Override
		public void handleDataChange(String dataPath, Object data) throws Exception {
			childrenDataMap.put(dataPath, (String)data);
			this.doNotify();
		}

		@Override
		public void handleDataDeleted(String dataPath) throws Exception {
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
