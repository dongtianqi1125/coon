package cn.ms.coon;

import cn.ms.coon.service.Mconf;
import cn.ms.neural.NURL;

public class MconfTest {

	public static void main(String[] args) {
		try {
			NURL nurl = NURL.valueOf("zookeeper://127.0.0.1:2181?session=5000");
			Mconf mconf = CoonFactory.CF.getCoon(nurl, Mconf.class);
			mconf.connect(nurl);
			System.out.println(mconf);
			
			Thread.sleep(1000000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
