package cn.ms.coon.zookeeper.transporter;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.neural.NURL;

public abstract class AbstractZkTransporter<TargetChildListener> implements ZkTransporter {

	private static final Logger logger = LoggerFactory.getLogger(AbstractZkTransporter.class);

	private NURL nurl;
	private volatile boolean closed = false;
	private final Set<StateListener> stateListeners = new CopyOnWriteArraySet<StateListener>();
	private final ConcurrentMap<String, ConcurrentMap<ChildListener, TargetChildListener>> childListeners = new ConcurrentHashMap<String, ConcurrentMap<ChildListener, TargetChildListener>>();


	@Override
	public void connect(NURL nurl) {
		this.nurl = nurl;
	}

	@Override
	public NURL getNurl() {
		return nurl;
	}

	@Override
	public void create(String path, boolean ephemeral) {
		int i = path.lastIndexOf('/');
		if (i > 0) {
			create(path.substring(0, i), false);
		}
		if (ephemeral) {
			createEphemeral(path);
		} else {
			createPersistent(path);
		}
	}
	
	@Override
	public void createData(String path, String json) {
		try {
			this.create(path, false);
			this.doCreateData(path, json);
		} catch (Exception e) {
		}		
	}

	@Override
	public void addStateListener(StateListener listener) {
		stateListeners.add(listener);
	}

	@Override
	public void removeStateListener(StateListener listener) {
		stateListeners.remove(listener);
	}

	public Set<StateListener> getSessionListeners() {
		return stateListeners;
	}

	@Override
	public List<String> addChildListener(String path, final ChildListener listener) {
		ConcurrentMap<ChildListener, TargetChildListener> listeners = childListeners.get(path);
		if (listeners == null) {
			childListeners.putIfAbsent(path, new ConcurrentHashMap<ChildListener, TargetChildListener>());
			listeners = childListeners.get(path);
		}
		TargetChildListener targetListener = listeners.get(listener);
		if (targetListener == null) {
			listeners.putIfAbsent(listener, createTargetChildListener(path, listener));
			targetListener = listeners.get(listener);
		}
		return addTargetChildListener(path, targetListener);
	}

	@Override
	public void removeChildListener(String path, ChildListener listener) {
		ConcurrentMap<ChildListener, TargetChildListener> listeners = childListeners.get(path);
		if (listeners != null) {
			TargetChildListener targetListener = listeners.remove(listener);
			if (targetListener != null) {
				removeTargetChildListener(path, targetListener);
			}
		}
	}

	protected void stateChanged(int state) {
		for (StateListener sessionListener : getSessionListeners()) {
			sessionListener.stateChanged(state);
		}
	}

	@Override
	public void close() {
		if (closed) {
			return;
		}
		closed = true;
		try {
			doClose();
		} catch (Throwable t) {
			logger.warn(t.getMessage(), t);
		}
	}

	protected abstract void doClose();
	protected abstract void createPersistent(String path);
	protected abstract void createEphemeral(String path);
	protected abstract void doCreateData(String path, String json);
	// ===== Listener Path Node
	protected abstract TargetChildListener createTargetChildListener(String path, ChildListener listener);
	protected abstract List<String> addTargetChildListener(String path, TargetChildListener listener);
	protected abstract void removeTargetChildListener(String path, TargetChildListener listener);
	// ===== Listener Path Data, 只监听子节点数据变更操作
	protected abstract void addDataListener(String path, DataListener listener);
	protected abstract void removeDataListener(String path, DataListener listener);
	
}
