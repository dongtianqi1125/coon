package cn.ms.coon.zookeeper;

import io.neural.NURL;

import java.util.Map;

import cn.ms.coon.zookeeper.transporter.CuratorZkTransporter;
import cn.ms.coon.zookeeper.transporter.ZkTransporter.DataListener;

public class CuratorZkTransporterTest {
	
	public static void main(String[] args) {
		try {
			CuratorZkTransporter transporter = new CuratorZkTransporter();
			transporter.connect(NURL.valueOf("zookeeper://127.0.0.1:2181"));
			
			String path = "/mconf/gateway/router";
			transporter.addDataListener(path, new DataListener() {
				@Override
				public void dataChanged(String path, Map<String, String> childrenDatas) {
					System.out.println(path+"==>"+childrenDatas);
				}
			});
			
			transporter.createData(path+"/url1", "a123456789");
			
			Thread.sleep(5000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
