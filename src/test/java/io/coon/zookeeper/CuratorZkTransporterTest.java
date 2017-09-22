package io.coon.zookeeper;

import io.coon.zookeeper.transporter.CuratorZkTransporter;
import io.coon.zookeeper.transporter.ZkTransporter.DataListener;
import io.neural.NURL;

import java.util.Map;

public class CuratorZkTransporterTest {
	
	public static void main(String[] args) {
		try {
			CuratorZkTransporter transporter = new CuratorZkTransporter();
			transporter.connect(NURL.valueOf("zookeeper://127.0.0.1:2181?group=mconf"));
			
			String path = "/mconf/gateway/router";
			transporter.addDataListener(path, new DataListener() {
				@Override
				public void dataChanged(String path, Map<String, String> childrenDatas) {
					System.out.println(path+"==>"+childrenDatas);
				}
			});
			
			Thread.sleep(2000);
			
			transporter.createData(path+"/url4", "dddd"+System.currentTimeMillis());
			
			Thread.sleep(2000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
