package cn.ms.coon.zookeeper;

import io.neural.NURL;
import io.neural.extension.Extension;
import io.neural.extension.ExtensionLoader;
import io.neural.extension.NSPI;

import java.util.ArrayList;
import java.util.HashMap;
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

import com.alibaba.fastjson.JSON;

/**
 * The base of Zookeeper Mconf.
 * 
 * @author lry
 */
@Extension("zookeeper")
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
		this.transporter.connect(nurl);
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
	public Map<String, Map<String, String>> apps() {
		Map<String, Map<String, String>> apps = new ConcurrentHashMap<String, Map<String, String>>();
		try {
			List<String> preApps = transporter.getChildren("/"+super.ROOT);
			for (String preApp:preApps) {
				NURL nurl = NURL.valueOf("/" + preApp);
				apps.put(nurl.getPath(), nurl.getParameters());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return apps;
	}
	
	public static void main(String[] args) {
		ZookeeperMconf zookeeperMconf = new ZookeeperMconf();
		zookeeperMconf.connect(NURL.valueOf("zookeeper://127.0.0.1:2181/mconf?session=5000"));
		System.out.println(zookeeperMconf.apps());
		System.out.println(zookeeperMconf.confs());
	}
	
	@Override
	public Map<String, Map<String, String>> confs() {
		Map<String, Map<String, String>> confs = new ConcurrentHashMap<String, Map<String, String>>();
		try {
			List<String> preApps = transporter.getChildren("/"+super.ROOT);
			for (String preApp:preApps) {
				NURL appNURL = NURL.valueOf("/" + preApp);
				List<String> preConfs = transporter.getChildren("/"+super.ROOT + "/" + preApp);
				for (String preConf:preConfs) {
					NURL confNURL = NURL.valueOf("/" + preConf);
					Map<String, String> attributes =new HashMap<String, String>();
					attributes.putAll(confNURL.getParameters());
					attributes.putAll(appNURL.getParameters());
					attributes.put(Consts.APPLICATION_KEY, appNURL.getPath());
					confs.put(confNURL.getPath(), attributes);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return confs;
	}

	@Override
	public void destroy() {
		this.transporter.close();
	}

}
