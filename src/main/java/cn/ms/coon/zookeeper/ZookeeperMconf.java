package cn.ms.coon.zookeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.support.Consts;
import cn.ms.coon.support.CoonListener;
import cn.ms.coon.support.mconf.AbstractMconf;
import cn.ms.coon.support.mconf.Mcf;
import cn.ms.coon.zookeeper.transporter.ZkTransporter;
import cn.ms.coon.zookeeper.transporter.ZkTransporter.DataListener;
import cn.ms.neural.NURL;
import cn.ms.neural.extension.ExtensionLoader;
import cn.ms.neural.extension.NSPI;

import com.alibaba.fastjson.JSON;

/**
 * The base of Zookeeper Mconf.
 * 
 * @author lry
 */
public class ZookeeperMconf extends AbstractMconf {

	private static final Logger logger = LoggerFactory.getLogger(ZookeeperMconf.class);

	private ZkTransporter transporter;
	
	@SuppressWarnings("rawtypes")
	private final Map<CoonListener, DataListener> coonListenerMap = new ConcurrentHashMap<CoonListener, DataListener>();
	
	@Override
	public void connect(NURL nurl) {
		super.connect(nurl);
		NSPI nspi = ZkTransporter.class.getAnnotation(NSPI.class);
		String transporter = nurl.getParameter(Consts.TRANSPORTER_KEY, nspi.value());
		this.transporter = ExtensionLoader.getLoader(ZkTransporter.class).getExtension(transporter);
	}
	
	@Override
	public boolean available() {
		return this.transporter.isConnected();
	}

	@Override
	public void publish(Mcf mcf, Object obj) {
		try{
			String path = mcf.buildRoot(super.nurl).getKey();
			String json = super.obj2Json(obj);
			
			logger.debug("The PATH[{}] add conf data[{}].", path, json);
			transporter.createData(path, json);
		} catch (Exception e) {
			throw new IllegalStateException("Publish data exception.", e);
		}
	}

	@Override
	public void unpublish(Mcf mcf, Object obj) {
		try {
			String path;
			if (isNotBlank(mcf.getData())) {
				path = mcf.buildRoot(super.nurl).getKey();
				logger.debug("The PATH[{}] delete conf data.", path);
			} else {
				path = mcf.buildRoot(super.nurl).getPrefixKey();
				logger.debug("The PATH[{}] and SubPATH delete conf datas.", path);
			}
			
			transporter.delete(path);
		} catch (Exception e) {
			throw new IllegalStateException("UnPublish data exception.", e);
		}
	}

	@Override
	public <T> void subscribe(Mcf mcf, final Class<T> cls, final CoonListener<T> listener) {
		DataListener dataListener = coonListenerMap.get(listener);
		if(dataListener != null){
			return;
		} else {
			coonListenerMap.put(listener, dataListener = new DataListener() {
				@Override
				public void dataChanged(String path, Map<String, String> childrenDatas) {
					if(childrenDatas!=null){
						List<T> list = new ArrayList<T>();
						for (Map.Entry<String, String> entry : childrenDatas.entrySet()) {
							list.add(JSON.parseObject(entry.getValue(), cls));
						}
						listener.notify(list);
					}
				}
			});
		}
		
		String path = mcf.buildRoot(super.nurl).getPrefixKey();
		if (super.isBlank(path)) {
			throw new RuntimeException("The PATH cannot be empty, path==" + path);
		} else {
			try {
				transporter.addDataListener(path, dataListener);
			} catch (Exception e) {
				throw new RuntimeException("The PATH[" + path + "] is exception.", e);
			}
		}
	}

	@Override
	public <T> void unsubscribe(Mcf mcf, CoonListener<T> listener) {
		DataListener dataListener = coonListenerMap.get(listener);
		if(dataListener == null){
			return;
		} else {
			String path = mcf.buildRoot(super.nurl).getPrefixKey();
			if (super.isBlank(path)) {
				throw new RuntimeException("The PATH cannot be empty, path==" + path);
			} else {
				try {
					transporter.removeDataListener(path, dataListener);
				} catch (Exception e) {
					throw new RuntimeException("The PATH[" + path + "] is exception.", e);
				}
			}
		}
	}
	
	@Override
	public <T> T lookup(Mcf mcf, Class<T> cls) {
		String path = mcf.buildRoot(super.nurl).getPrefixKey();
		logger.debug("The PATH[{}] lookups conf data.", path);
		if (super.isBlank(path)) {
			throw new RuntimeException("The PATH cannot be empty, path==" + path);
		} else {
			String json = transporter.getData(path);
			return JSON.parseObject(json, cls);
		}
	}

	@Override
	public <T> List<T> lookups(Mcf mcf, Class<T> cls) {
		String path = mcf.buildRoot(super.nurl).getPrefixKey();
		logger.debug("The PATH[{}] lookups conf data.", path);
		if (super.isBlank(path)) {
			throw new RuntimeException("The PATH cannot be empty, path==" + path);
		} else {
			List<T> list = new ArrayList<T>();
			List<String> jsonList = transporter.getChildrenData(path);
			for (String json:jsonList) {
				list.add(JSON.parseObject(json, cls));
			}
			
			return list;
		}
	}

	@Override
	public void destroy() {
		this.transporter.close();
	}

}
