package cn.ms.coon.support.mreg.monitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.service.Mreg;
import cn.ms.coon.support.Consts;
import cn.ms.coon.support.CoonListener;
import cn.ms.coon.support.mreg.MregCommon;
import cn.ms.neural.NURL;

public class MregGovernor implements CoonListener<NURL> {

	private static final Logger logger = LoggerFactory.getLogger(MregGovernor.class);
	private static final NURL SUBSCRIBE = new NURL(Consts.ADMIN_PROTOCOL, MregCommon.getLocalHost(), 0, "", 
			//允许以interface,group,version,classifier作为条件查询
			Consts.INTERFACE_KEY, Consts.ANY_VALUE,// 服务ID
			Consts.GROUP_KEY, Consts.ANY_VALUE,// 分组 
			Consts.VERSION_KEY, Consts.ANY_VALUE,// 版本号
			Consts.CLASSIFIER_KEY, Consts.ANY_VALUE,// 分类器
			
			// 订阅providers,consumers,routers,configurators
			Consts.CATEGORY_KEY, Consts.PROVIDERS_CATEGORY + "," + 
			Consts.CONSUMERS_CATEGORY + "," + Consts.ROUTERS_CATEGORY + "," + Consts.CONFIGURATORS_CATEGORY, 
			Consts.ENABLED_KEY, Consts.ANY_VALUE,// enabled=true表示覆盖规则是否生效，可不填，缺省生效。 
			Consts.CHECK_KEY, String.valueOf(false));// check=false表示订阅失败后不报错，在后台定时重试

	private Mreg mreg;
	private static final AtomicLong ID = new AtomicLong();
	private final ConcurrentHashMap<String, Long> NURL_IDS_MAPPER = new ConcurrentHashMap<String, Long>();
	// ConcurrentMap<category, ConcurrentMap<servicename, Map<Long, NURL>>>
	private final ConcurrentMap<String, ConcurrentMap<String, Map<Long, NURL>>> registryCache = new ConcurrentHashMap<String, ConcurrentMap<String, Map<Long, NURL>>>();

    public void start(Mreg mreg) {
    	this.mreg = mreg;
    	new Thread(new Runnable() {
    		@Override
    		public void run() {
    			logger.info("订阅服务:{}", SUBSCRIBE.toFullString());
    			MregGovernor.this.mreg.subscribe(SUBSCRIBE, MregGovernor.this);
    		}
    	}, "SUBSCRIBE-MSUI-THREAD").start();
	}
    
	public void destroy() throws Exception {
		mreg.unsubscribe(SUBSCRIBE, this);
	}

	public ConcurrentMap<String, ConcurrentMap<String, Map<Long, NURL>>> getRegistryCache() {
		return registryCache;
	}

	public void update(NURL oldNURL, NURL newNURL) {
		mreg.unregister(oldNURL);
		mreg.register(newNURL);
	}

	public void unregister(NURL nurl) {
		NURL_IDS_MAPPER.remove(nurl.toFullString());
		mreg.unregister(nurl);
	}

	public void register(NURL nurl) {
		mreg.register(nurl);
	}

	/**
	 * 收到的通知对于 ，同一种类型数据（override、subcribe、route、其它是Provider），同一个服务的数据是全量的
	 */
	@Override
	public void notify(List<NURL> nurls) {
		if (nurls == null || nurls.isEmpty()) {
			return;
		}

		final Map<String, Map<String, Map<Long, NURL>>> categories = new HashMap<String, Map<String, Map<Long, NURL>>>();
		for (NURL nurl : nurls) {
			String category = nurl.getParameter(Consts.CATEGORY_KEY, Consts.PROVIDERS_CATEGORY);
			if (Consts.EMPTY_PROTOCOL.equalsIgnoreCase(nurl.getProtocol())) { // 注意：empty协议的group和version为*
				ConcurrentMap<String, Map<Long, NURL>> services = registryCache.get(category);
				if (services != null) {
					String group = nurl.getParameter(Consts.GROUP_KEY);
					String version = nurl.getParameter(Consts.VERSION_KEY);

					// 注意：empty协议的group和version为*
					if (!Consts.ANY_VALUE.equals(group) && !Consts.ANY_VALUE.equals(version)) {
						services.remove(nurl.getServiceKey());
					} else {
						for (Map.Entry<String, Map<Long, NURL>> serviceEntry : services.entrySet()) {
							String service = serviceEntry.getKey();
							if (getInterface(service).equals(nurl.getServiceInterface()) && (Consts.ANY_VALUE.equals(group) 
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

				String service = generateServiceKey(nurl);
				Map<Long, NURL> ids = services.get(service);
				if (ids == null) {
					ids = new HashMap<Long, NURL>();
					services.put(service, ids);
				}

				// 保证ID对于同一个NURL的不可变
				if (NURL_IDS_MAPPER.containsKey(nurl.toFullString())) {
					ids.put(NURL_IDS_MAPPER.get(nurl.toFullString()), nurl);
				} else {
					long currentId = ID.incrementAndGet();
					ids.put(currentId, nurl);
					NURL_IDS_MAPPER.putIfAbsent(nurl.toFullString(), currentId);
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
	 * NURL转为serviceKey=interface/group:version
	 * 
	 * @param nurl
	 * @return
	 */
	private String generateServiceKey(NURL nurl) {
		String inf = nurl.getServiceInterface();
		if (inf == null) {
			return null;
		}

		StringBuilder buf = new StringBuilder();
		buf.append(inf).append("/");

		String group = nurl.getParameter(Consts.GROUP_KEY);
		if (group != null && group.length() > 0) {
			buf.append(group);
		}

		String version = nurl.getParameter(Consts.VERSION_KEY);
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
	private String getInterface(String serviceKey) {
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
	private String getGroup(String serviceKey) {
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
	private String getVersion(String serviceKey) {
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

}