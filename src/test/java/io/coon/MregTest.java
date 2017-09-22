package io.coon;

import io.coon.Coon;
import io.coon.api.Mreg;
import io.coon.support.mreg.MregGovernor;
import io.neural.NURL;

import com.alibaba.fastjson.JSON;

public class MregTest {

	public static void main(String[] args) {
		try {
			NURL nurl = NURL.valueOf("zookeeper://127.0.0.1:2181?session=5000");
			Mreg mreg = Coon.CF.getCoon(nurl, Mreg.class);
			mreg.connect(nurl);
			
			mreg.register(NURL.valueOf("ms://10.14.23.42:8080/cn.ms.coon.UserService?application=gateway&version=1.0.0&group=weixin&category=providers"));
			mreg.register(NURL.valueOf("ms://10.14.23.43:8080/cn.ms.coon.OrderService?application=gateway&version=1.0.0&group=weixin&category=providers"));
			mreg.register(NURL.valueOf("ms://10.14.23.44:8080/cn.ms.coon.GoodsService?version=1.0.0&group=weixin&category=providers"));
			mreg.register(NURL.valueOf("ms://10.14.23.45:8080/cn.ms.coon.PayService?application=router&version=1.0.0&group=weixin&category=providers"));
			mreg.register(NURL.valueOf("ms://10.14.23.46:8080/cn.ms.coon.UnipayService?version=1.0.0&group=weixin&category=providers"));
			mreg.register(NURL.valueOf("ms://10.14.23.47:8080/cn.ms.coon.UserService?application=gateway&version=1.0.0&group=weixin&category=providers"));
			mreg.register(NURL.valueOf("ms://10.14.23.48:8080/cn.ms.coon.UserService?application=gateway&version=1.0.0&group=weixin&category=consumers"));
			mreg.register(NURL.valueOf("ms://10.14.23.49:8080/cn.ms.coon.UserService?version=1.0.0&group=weixin&category=consumers"));
			mreg.register(NURL.valueOf("ms://10.14.23.50:8080/cn.ms.coon.AdminService?application=admin&version=1.0.0&group=weixin&category=consumers"));
			
			MregGovernor mregGovernor = new MregGovernor(mreg);
			Thread.sleep(3000);
			System.out.println(JSON.toJSONString(mregGovernor.getServices()));
			Thread.sleep(1000000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
