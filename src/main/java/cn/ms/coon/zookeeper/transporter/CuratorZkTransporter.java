package cn.ms.coon.zookeeper.transporter;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;

import cn.ms.coon.support.Consts;
import cn.ms.neural.NURL;
import cn.ms.neural.extension.Extension;

@Extension("curator")
public class CuratorZkTransporter extends AbstractZkTransporter<CuratorWatcher> {

	private CuratorFramework client;

	@Override
	public void connect(NURL url) {
		super.connect(url);
		Builder builder = CuratorFrameworkFactory.builder()
				.connectString(url.getBackupAddress())
				.retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000))
				.connectionTimeoutMs(url.getParameter(Consts.TIMEOUT_KEY, Consts.DEFAULT_REGISTRY_CONNECT_TIMEOUT))
                .sessionTimeoutMs(url.getParameter(Consts.SESSION_TIMEOUT_KEY, Consts.DEFAULT_SESSION_TIMEOUT));
		
		String authority = url.getAuthority();
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
					} else if (state == ConnectionState.RECONNECTED) {
						CuratorZkTransporter.this.stateChanged(StateListener.RECONNECTED);
					}
				}
			});
		client.start();
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
	public void createEphemeral(String path) {
		try {
			client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
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
			return null;
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

}
