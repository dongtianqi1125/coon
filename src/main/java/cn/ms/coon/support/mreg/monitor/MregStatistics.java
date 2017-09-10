package cn.ms.coon.support.mreg.monitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.ms.coon.Mreg;
import cn.ms.coon.support.AbstractCoonFactory;
import cn.ms.coon.support.mreg.AbstractMreg;
import cn.ms.coon.support.mreg.MregCommon;
import cn.ms.neural.NURL;

/**
 * The Registry Statistics.
 * 
 * @author lry
 */
public class MregStatistics {

	/**
	 * 服务器地址
	 */
	public static final String SERVER_KEY = "server";
	/**
	 * HostName
	 */
	public static final String HOSTNAME_KEY = "hostName";
	/**
	 * 注册中心连接状况
	 */
	public static final String AVAILABLE_KEY = "available";
	/**
	 * 注册中心已经连接
	 */
	public static final String AVAILABLE_CONNECTED_VAL = "Connected";
	/**
	 * 注册中心已经断开连接
	 */
	public static final String AVAILABLE_DISCONNECTED_VAL = "Disconnected";
	/**
	 * 已注册数量
	 */
	public static final String REGISTEREDCOUNT_KEY = "registeredCount";
	/**
	 * 已订阅数量
	 */
	public static final String SUBSCRIBEDCOUNT_KEY = "subscribedCount";

	
	/**
	 * 注册中心清单
	 */
	public static final String REGISTRIES_KEY = "registries";
	/**
	 * 提供者清单
	 */
	public static final String PROVIDERS_KEY = "providers";
	/**
	 * 消费者清单
	 */
	public static final String CONSUMERS_KEY = "consumers";

	
	/**
	 * 获取注册中心列表
	 * 
	 * @param url
	 * @return
	 */
	public static Map<String, Object> getRegistries(NURL url) {
		Map<String, Object> dataMap = new HashMap<String, Object>();

		Collection<Mreg> mregs = AbstractCoonFactory.getCoons(Mreg.class);
		int registeredCount = 0;
		int subscribedCount = 0;
		if (mregs != null && mregs.size() > 0) {
			for (Mreg mreg : mregs) {
				Map<String, Object> tempMap = new HashMap<String, Object>();

				String server = mreg.getNurl().getAddress();
				tempMap.put(SERVER_KEY, server);
				tempMap.put(HOSTNAME_KEY, MregCommon.getHostName(server));
				tempMap.put(AVAILABLE_KEY, mreg.available() ? AVAILABLE_CONNECTED_VAL : AVAILABLE_DISCONNECTED_VAL);

				int registeredSize = 0;
				int subscribedSize = 0;
				if (mreg instanceof AbstractMreg) {
					// 已注册Registered
					registeredSize = ((AbstractMreg) mreg).getRegistered().size();
					tempMap.put(REGISTEREDCOUNT_KEY, registeredSize);
					registeredCount += registeredSize;

					// 已订阅Subscribed
					subscribedSize = ((AbstractMreg) mreg).getSubscribed().size();
					tempMap.put(SUBSCRIBEDCOUNT_KEY, subscribedSize);
					subscribedCount += subscribedSize;
				}

				dataMap.put(server, tempMap);
			}
			dataMap.put(REGISTEREDCOUNT_KEY, registeredCount);
			dataMap.put(SUBSCRIBEDCOUNT_KEY, subscribedCount);
		}

		return dataMap;
	}

	/**
	 * 获取服务提供者列表
	 * 
	 * @param url
	 * @return
	 */
	public static Map<String, Object> getProviders(NURL url) {
		Map<String, Object> dataMap = new HashMap<String, Object>();
		String registryAddress = url.getParameter("registry", "");

		// 获取所有的注册中心地址
		Mreg mreg = null;
		Collection<Mreg> mregs = AbstractCoonFactory.getCoons(Mreg.class);
		if (mregs != null && mregs.size() > 0) {
			List<String> registryList = new ArrayList<String>();
			for (Mreg r : mregs) {
				String sp = r.getNurl().getAddress();
				registryList.add(sp);
				if (((registryAddress == null || registryAddress.length() == 0) && mreg == null) || registryAddress.equals(sp)) {
					mreg = r;
				}
			}
			dataMap.put(REGISTRIES_KEY, registryList);
		}

		// 获取所有已注册的服务地址
		if (mreg instanceof AbstractMreg) {
			Set<NURL> services = ((AbstractMreg) mreg).getRegistered();
			if (services != null && services.size() > 0) {
				List<String> providers = new ArrayList<String>();
				for (NURL u : services) {
					providers.add(u.toFullString());
				}
				dataMap.put(PROVIDERS_KEY, providers);
			}
		}

		return dataMap;
	}

	/**
	 * 获取消费者列表
	 * 
	 * @param url
	 * @return
	 */
	public static Map<String, Object> getConsumers(NURL url) {
		Map<String, Object> dataMap = new HashMap<String, Object>();
		String registryAddress = url.getParameter("registry", "");

		Mreg mreg = null;
		Collection<Mreg> mregs = AbstractCoonFactory.getCoons(Mreg.class);
		if (mregs != null && mregs.size() > 0) {
			List<String> registryList = new ArrayList<String>();
			for (Mreg r : mregs) {
				String sp = r.getNurl().getAddress();
				registryList.add(sp);
				if (((registryAddress == null || registryAddress.length() == 0) && mreg == null) || registryAddress.equals(sp)) {
					mreg = r;
				}
			}
			dataMap.put(REGISTRIES_KEY, registryList);
		}

		if (mreg instanceof AbstractMreg) {
			Set<NURL> services = ((AbstractMreg) mreg).getSubscribed().keySet();
			if (services != null && services.size() > 0) {
				List<String> consumers = new ArrayList<String>();
				for (NURL u : services) {
					consumers.add(u.toFullString());
				}
				dataMap.put(CONSUMERS_KEY, consumers);
			}
		}

		return dataMap;
	}

}