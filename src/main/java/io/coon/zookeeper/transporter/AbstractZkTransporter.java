package io.coon.zookeeper.transporter;

import io.neural.NURL;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractZkTransporter<TargetChildListener> implements ZkTransporter {

	private static final Logger logger = LoggerFactory.getLogger(AbstractZkTransporter.class);

	private NURL nurl;
	private volatile boolean closed = false;
	protected final CountDownLatch countDownLatch = new CountDownLatch(1);
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
			this.create(path.substring(0, i), false);
		}
		if (ephemeral) {
			this.createEphemeral(path);
		} else {
			this.createPersistent(path);
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
	public List<String> getChildrenData(String path) {
		List<String> childrenDatas = new ArrayList<String>();
		
		try {
			List<String> childrens = this.getChildren(path);
			for (String children:childrens) {
				String json = this.doGetChildrenData(path + "/" + children);
				if(json == null || json.length() < 1){
					continue;
				}
				childrenDatas.add(json);
			}
		} catch (Exception e) {
		}
		
		return childrenDatas;
	}
	
	@Override
	public String getData(String path) {
		return doGetChildrenData(path);
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
	protected abstract String doGetChildrenData(String path);
	protected abstract TargetChildListener createTargetChildListener(String path, ChildListener listener);
	protected abstract List<String> addTargetChildListener(String path, TargetChildListener listener);
	protected abstract void removeTargetChildListener(String path, TargetChildListener listener);
	
}
