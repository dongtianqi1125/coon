package cn.ms.coon.support.mreg.monitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.CoonFactory;
import cn.ms.coon.Mreg;
import cn.ms.coon.support.Consts;
import cn.ms.coon.support.CoonListener;
import cn.ms.coon.support.mreg.MregCommon;
import cn.ms.neural.NURL;
import cn.ms.neural.extension.ExtensionLoader;

/**
 * 注册中心治理<br>
 * <br>
 * 功能清单：<br>
 * 1.获取当前的注册中心列表<br>
 * 2.获取所有的应用列表<br>
 * 3.订阅所有的服务
 * 
 * 4.获取所有的服务提供者表列表<br>
 * 5.获取所有的服务消费者列表<br>
 * 
 * 6.获取指定的服务提供者信息<br>
 * 7.获取指定的服务消费者信息<br>
 * 
 * 8.搜索服务提供者<br>
 * 9.搜索服务消费者<br>
 * 10.<br>
 * 11.<br>
 * 
 * @author lry
 */
public class MregGovernor implements CoonListener<NURL> {

	private static final Logger logger = LoggerFactory.getLogger(MregGovernor.class);

	private static final NURL SUBSCRIBE = new NURL(Consts.ADMIN_PROTOCOL, MregCommon.getLocalHost(), 0, "", 
			//允许以interface,group,version,classifier作为条件查询
			Consts.INTERFACE_KEY, Consts.ANY_VALUE,// 服务ID
			Consts.GROUP_KEY, Consts.ANY_VALUE,// 分组 
			Consts.VERSION_KEY, Consts.ANY_VALUE,// 版本号
			Consts.CLASSIFIER_KEY, Consts.ANY_VALUE,// 分类器
			
			// 订阅providers,consumers,routers,configurators
			Consts.CATEGORY_KEY, Consts.PROVIDERS_CATEGORY + "," + Consts.CONSUMERS_CATEGORY + "," + Consts.ROUTERS_CATEGORY + "," + Consts.CONFIGURATORS_CATEGORY, 
			Consts.ENABLED_KEY, Consts.ANY_VALUE,// enabled=true表示覆盖规则是否生效，可不填，缺省生效。 
			Consts.CHECK_KEY, String.valueOf(false));// check=false表示订阅失败后不报错，在后台定时重试

	private NURL url;
	private Mreg mreg;
	private static final AtomicLong ID = new AtomicLong();

	// ConcurrentMap<category, ConcurrentMap<servicename, Map<Long, EURL>>>
	private final ConcurrentMap<String, ConcurrentMap<String, Map<Long, NURL>>> registryCache = new ConcurrentHashMap<String, ConcurrentMap<String, Map<Long, NURL>>>();

	private final ConcurrentHashMap<String, Long> EURL_IDS_MAPPER = new ConcurrentHashMap<String, Long>();

	public boolean start(NURL url) throws Exception {
		this.url = url;
		CoonFactory coonFactory = ExtensionLoader.getLoader(CoonFactory.class).getExtension(this.url.getProtocol());
		mreg = coonFactory.getCoon(url, Mreg.class);

		int connectTimeout = this.url.getParameter(Consts.CONNECT_TIMEOUT_KEY, 60000) / 1000;
		while (!mreg.available() && connectTimeout-- > 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (mreg.available()) {
			logger.info("注册中心连接成功!!!");
		} else {
			logger.warn("注册中心连接失败!!!");
		}

		new Thread(new Runnable() {
			@Override
			public void run() {
				logger.info("订阅服务:{}", SUBSCRIBE.toFullString());
				mreg.subscribe(SUBSCRIBE, MregGovernor.this);
			}
		}, "SUBSCRIBE-MSUI-THREAD").start();

		return mreg.available();
	}

	public void destroy() throws Exception {
		mreg.unsubscribe(SUBSCRIBE, this);
	}

	public ConcurrentMap<String, ConcurrentMap<String, Map<Long, NURL>>> getRegistryCache() {
		return registryCache;
	}

	public void update(NURL oldEURL, NURL newEURL) {
		mreg.unregister(oldEURL);
		mreg.register(newEURL);
	}

	public void unregister(NURL url) {
		EURL_IDS_MAPPER.remove(url.toFullString());
		mreg.unregister(url);
	}

	public void register(NURL url) {
		mreg.register(url);
	}

	// 收到的通知对于 ，同一种类型数据（override、subcribe、route、其它是Provider），同一个服务的数据是全量的
	@Override
	public void notify(List<NURL> urls) {
		if (urls == null || urls.isEmpty()) {
			return;
		}

		final Map<String, Map<String, Map<Long, NURL>>> categories = new HashMap<String, Map<String, Map<Long, NURL>>>();
		for (NURL url : urls) {
			String category = url.getParameter(Consts.CATEGORY_KEY, Consts.PROVIDERS_CATEGORY);
			if (Consts.EMPTY_PROTOCOL.equalsIgnoreCase(url.getProtocol())) { // 注意：empty协议的group和version为*
				ConcurrentMap<String, Map<Long, NURL>> services = registryCache.get(category);
				if (services != null) {
					String group = url.getParameter(Consts.GROUP_KEY);
					String version = url.getParameter(Consts.VERSION_KEY);

					// 注意：empty协议的group和version为*
					if (!Consts.ANY_VALUE.equals(group) && !Consts.ANY_VALUE.equals(version)) {
						services.remove(url.getServiceKey());
					} else {
						for (Map.Entry<String, Map<Long, NURL>> serviceEntry : services.entrySet()) {
							String service = serviceEntry.getKey();
							if (getInterface(service).equals(url.getServiceInterface()) && (Consts.ANY_VALUE.equals(group) 
									|| MregCommon.isEquals(group, getGroup(service))) && (Consts.ANY_VALUE.equals(version) 
									|| MregCommon.isEquals(version, getVersion(service)))) {
								services.remove(service);
							}
						}
					}
				}
			} else {
				Map<String, Map<Long, NURL>> services = categories.get(category);
				if (services == null) {
					services = new HashMap<String, Map<Long, NURL>>();
					categories.put(category, services);
				}

				String service = generateServiceKey(url);
				Map<Long, NURL> ids = services.get(service);
				if (ids == null) {
					ids = new HashMap<Long, NURL>();
					services.put(service, ids);
				}

				// 保证ID对于同一个EURL的不可变
				if (EURL_IDS_MAPPER.containsKey(url.toFullString())) {
					ids.put(EURL_IDS_MAPPER.get(url.toFullString()), url);
				} else {
					long currentId = ID.incrementAndGet();
					ids.put(currentId, url);
					EURL_IDS_MAPPER.putIfAbsent(url.toFullString(), currentId);
				}
			}
		}

		for (Map.Entry<String, Map<String, Map<Long, NURL>>> categoryEntry : categories.entrySet()) {
			String category = categoryEntry.getKey();
			ConcurrentMap<String, Map<Long, NURL>> services = registryCache.get(category);
			if (services == null) {
				services = new ConcurrentHashMap<String, Map<Long, NURL>>();
				registryCache.put(category, services);
			}
			services.putAll(categoryEntry.getValue());
		}
	}

	/**
	 * EURL转为serviceKey=interface/group:version
	 * 
	 * @param url
	 * @return
	 */
	public static String generateServiceKey(NURL url) {
		String inf = url.getServiceInterface();
		if (inf == null) {
			return null;
		}

		StringBuilder buf = new StringBuilder();
		buf.append(inf).append("/");

		String group = url.getParameter(Consts.GROUP_KEY);
		if (group != null && group.length() > 0) {
			buf.append(group);
		}

		String version = url.getParameter(Consts.VERSION_KEY);
		if (version != null && version.length() > 0) {
			buf.append(":").append(version);
		}
		
		return buf.toString();
	}

	/**
	 * 从serviceKey获取interface<br>
	 * <br>
	 * 数据结构为serviceKey=interface/group:version
	 * 
	 * @param serviceKey
	 * @return
	 */
	public static String getInterface(String serviceKey) {
		if (MregCommon.isEmpty(serviceKey)) {
			throw new IllegalArgumentException("serviceKey must not be null");
		}
		
		int groupIndex = serviceKey.indexOf("/");
		if (groupIndex > 0) {
			return serviceKey.substring(0, groupIndex);
		} else {
			return null;
		}
	}

	/**
	 * 从serviceKey获取group<br>
	 * <br>
	 * 数据结构为serviceKey=interface/group:version
	 * 
	 * @param serviceKey
	 * @return
	 */
	public static String getGroup(String serviceKey) {
		if (MregCommon.isEmpty(serviceKey)) {
			throw new IllegalArgumentException("serviceKey must not be null");
		}
		
		int groupIndex = serviceKey.indexOf("/");
		int versionIndex = serviceKey.indexOf(":");
		if (groupIndex > 0 && versionIndex > 0) {
			return serviceKey.substring(groupIndex + 1, versionIndex);
		} else if (groupIndex > 0 && versionIndex < 0) {
			return serviceKey.substring(groupIndex + 1);
		} else if (groupIndex < 0 && versionIndex > 0) {
			return serviceKey.substring(0, versionIndex);
		} else {
			return serviceKey;
		}
	}

	/**
	 * 从serviceKey获取version<br>
	 * <br>
	 * 数据结构为serviceKey=interface/group:version
	 * 
	 * @param serviceKey
	 * @return
	 */
	public static String getVersion(String serviceKey) {
		if (MregCommon.isEmpty(serviceKey)) {
			throw new IllegalArgumentException("serviceKey must not be null");
		}
		
		int versionIndex = serviceKey.indexOf(":");
		if (versionIndex > 0) {
			return serviceKey.substring(versionIndex + 1);
		} else {
			return null;
		}
	}

	/**
	 * URL参数串转为Map
	 * 
	 * @param params
	 * @return
	 */
	public static Map<String, String> convertParametersMap(String params) {
		return MregCommon.parseQueryString(params);
	}

	/**
	 * serviceKey转为Map
	 * 
	 * @param serviceKey
	 * @return
	 */
	public static Map<String, String> serviceName2Map(String serviceKey) {
		Map<String, String> params = new HashMap<String, String>();
		if (MregCommon.isEmpty(serviceKey)) {
			return params;
		}
		
		int groupIndex = serviceKey.indexOf("/");
		int versionIndex = serviceKey.indexOf(":");
		if (groupIndex > 0) {
			params.put(Consts.GROUP_KEY, serviceKey.substring(0, groupIndex));
		}
		if (versionIndex > 0) {
			params.put(Consts.VERSION_KEY,
					serviceKey.substring(versionIndex + 1));
		}
		params.put(Consts.INTERFACE_KEY, getInterface(serviceKey));
		
		return params;
	}

}
