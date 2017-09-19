package cn.ms.coon.support.mreg;

import io.neural.NURL;
import io.neural.util.ConcurrentHashSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.service.Mreg;
import cn.ms.coon.support.Consts;
import cn.ms.coon.support.CoonListener;

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
	private final Map<String, Map<String, Map<Long, NURL>>> registryCache = new ConcurrentHashMap<String, Map<String, Map<Long, NURL>>>();

    public MregGovernor(Mreg mreg) {
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

	public Map<String, Map<String, Map<Long, NURL>>> getRegistryCache() {
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
				Map<String, Map<Long, NURL>> services = registryCache.get(category);
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
			Map<String, Map<Long, NURL>> services = registryCache.get(category);
			if (services == null) {
				services = new ConcurrentHashMap<String, Map<Long, NURL>>();
				registryCache.put(category, services);
			}
			services.putAll(categoryEntry.getValue());
		}
		
		this.doNotifyAnalysis();
	}
	
	public final static String HOSTS = "hosts";
	public final static String INSTANCES = "instances";
	private Map<String, Application>  applications = new ConcurrentHashMap<String, MregGovernor.Application>();
	// ConcurrentMap<servicename, ConcurrentMap<category, Map<Long, NURL>>>
	private Map<String, Map<String, ServiceUnit>> services = new ConcurrentHashMap<String, Map<String, ServiceUnit>>();

	public Map<String, Application> getApplications() {
		return applications;
	}
	
	public Map<String, Map<String, ServiceUnit>> getServices() {
		return services;
	}
	
	private void doNotifyAnalysis() {
		// ConcurrentMap<category, ConcurrentMap<servicename, Map<Long, NURL>>>
		Map<String, Map<String, Map<Long, NURL>>> registryCache = this.getRegistryCache();
		for (Map.Entry<String, Map<String, Map<Long, NURL>>> categoryEntry : registryCache.entrySet()) {
			for (Map.Entry<String, Map<Long, NURL>> serviceEntry : categoryEntry.getValue().entrySet()) {
				Map<String, ServiceUnit> serviceMap = services.get(serviceEntry.getKey());
				if(serviceMap == null){
					services.put(serviceEntry.getKey(), serviceMap = new ConcurrentHashMap<String, ServiceUnit>());
				}
				
				ServiceUnit store = serviceMap.get(categoryEntry.getKey());
				if(store == null){
					serviceMap.put(categoryEntry.getKey(), store = new ServiceUnit());
				}
				
				Map<Long, NURL> categoryMap = store.getStatistics();
				categoryMap.putAll(serviceEntry.getValue());// 统计
				Map<String, Set<String>> analysisMap = store.getAnalysis();
				for (NURL nurl:serviceEntry.getValue().values()) {//分析
					analysisMap.get(HOSTS).add(nurl.getHost());
					analysisMap.get(INSTANCES).add(nurl.getAddress());
					
					//应用分析
					String app = nurl.getParameter(Consts.APPLICATION_KEY, Consts.APPLICATION_DEFAULT_VALUE);
					Application application = applications.get(app);
					if(application == null){
						applications.put(app, application = new Application());
						application.setApp(app);
					}
					if(Consts.PROVIDERS_CATEGORY.equals(categoryEntry.getKey())){
						application.getProviders().add(nurl);
					} else if(Consts.CONSUMERS_CATEGORY.equals(categoryEntry.getKey())){
						application.getConsumers().add(nurl);
					}
					String node = nurl.getParameter(Consts.NODE_KEY, Consts.NODE_DEFAULT_VALUE);
					store.getNodes().add(node);
					application.getNodes().add(node);
					String env = nurl.getParameter(Consts.ENV_KEY, Consts.ENV_DEFAULT_VALUE);
					store.getEnvs().add(env);
					application.getEnvs().add(env);
				}
			}
		}
	}
	
	public class Application {
		private String app;
		private Set<String> nodes = new ConcurrentHashSet<String>();
		private Set<String> envs = new ConcurrentHashSet<String>();
		private Set<NURL> providers = new ConcurrentHashSet<NURL>();
		private Set<NURL> consumers = new ConcurrentHashSet<NURL>();
		private Set<String> confs = new ConcurrentHashSet<String>();
		
		public Set<String> getNodes() {
			return nodes;
		}
		public void setNodes(Set<String> nodes) {
			this.nodes = nodes;
		}
		public Set<String> getEnvs() {
			return envs;
		}
		public void setEnvs(Set<String> envs) {
			this.envs = envs;
		}
		public String getApp() {
			return app;
		}
		public void setApp(String app) {
			this.app = app;
		}
		public Set<NURL> getProviders() {
			return providers;
		}
		public void setProviders(Set<NURL> providers) {
			this.providers = providers;
		}
		public Set<NURL> getConsumers() {
			return consumers;
		}
		public void setConsumers(Set<NURL> consumers) {
			this.consumers = consumers;
		}
		public Set<String> getConfs() {
			return confs;
		}
		public void setConfs(Set<String> confs) {
			this.confs = confs;
		}
		@Override
		public String toString() {
			return "Application [app=" + app + ", nodes=" + nodes + ", envs="
					+ envs + ", providers=" + providers + ", consumers="
					+ consumers + ", confs=" + confs + "]";
		}
	}
	
	public class ServiceUnit {
		Set<String> nodes = new ConcurrentHashSet<String>();
		Set<String> envs = new ConcurrentHashSet<String>();
		Map<String, Set<String>> analysis = new ConcurrentHashMap<String, Set<String>>();
		Map<Long, NURL> statistics = new ConcurrentHashMap<Long, NURL>();
		
		public ServiceUnit() {
			analysis.put(HOSTS, new ConcurrentHashSet<String>());
			analysis.put(INSTANCES, new ConcurrentHashSet<String>());
		}
		
		public Set<String> getNodes() {
			return nodes;
		}
		
		public void setNodes(Set<String> nodes) {
			this.nodes = nodes;
		}
		
		public Set<String> getEnvs() {
			return envs;
		}
		
		public void setEnvs(Set<String> envs) {
			this.envs = envs;
		}
		
		public Map<String, Set<String>> getAnalysis() {
			return analysis;
		}
		public void setAnalysis(Map<String, Set<String>> analysis) {
			this.analysis = analysis;
		}
		public Map<Long, NURL> getStatistics() {
			return statistics;
		}
		public void setStatistics(Map<Long, NURL> statistics) {
			this.statistics = statistics;
		}

		@Override
		public String toString() {
			return "ServiceUnit [nodes=" + nodes + ", envs=" + envs
					+ ", analysis=" + analysis + ", statistics=" + statistics
					+ "]";
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