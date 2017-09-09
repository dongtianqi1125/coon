package cn.ms.coon.zookeeper;

import java.util.Map;

import cn.ms.coon.zookeeper.transporter.ZkTransporter.DataListener;
import cn.ms.coon.zookeeper.transporter.ZkclientZkTransporter;
import cn.ms.neural.NURL;

public class ZkclientZkTransporterTest {

	
	public static void main(String[] args) {
		try {
			ZkclientZkTransporter transporter = new ZkclientZkTransporter();
			transporter.connect(NURL.valueOf("zookeeper://127.0.0.1:2181"));
			
			String path = "/mconf/gateway/router";
			transporter.addDataListener(path, new DataListener() {
				@Override
				public void dataChanged(String path, Map<String, String> childrenDatas) {
					System.out.println(path+"==>"+childrenDatas);
				}
			});
			
			transporter.createData(path+"/url1", "a123456789222");
			
			Thread.sleep(1000);
			
			transporter.createData(path+"/"+System.currentTimeMillis(), "a123456789222");
			
			Thread.sleep(5000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
