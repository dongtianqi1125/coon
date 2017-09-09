package cn.ms.coon.governor;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import cn.ms.coon.support.mreg.monitor.MregGovernor;
import cn.ms.neural.NURL;

import com.alibaba.fastjson.JSON;

public class MregGovernorTest {

	public static void main(String[] args) {
		try {
			MregGovernor mregGovernor = new MregGovernor();
			mregGovernor.start(NURL.valueOf("zookeeper://127.0.0.1:2181/?group=edsp"));
			Thread.sleep(3000);
			
			for (Map.Entry<String, ConcurrentMap<String, Map<Long, NURL>>> entry:mregGovernor.getRegistryCache().entrySet()) {
				for (Map.Entry<String, Map<Long, NURL>> entry2:entry.getValue().entrySet()) {
					System.out.println(entry2.getKey()+ "--->" + JSON.toJSONString(entry2));
				}
				
				return;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
