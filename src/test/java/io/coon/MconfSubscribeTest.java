package io.coon;

import io.coon.api.Mconf;
import io.coon.mconf.ParameterEntity;
import io.coon.support.CoonListener;
import io.coon.support.mconf.Mcf;
import io.neural.NURL;

import java.sql.Timestamp;
import java.util.List;

public class MconfSubscribeTest {

	public static void main(String[] args) {
		try {
			NURL nurl = NURL.valueOf("zookeeper://127.0.0.1:2181?session=5000");
			Mconf mconf = Coon.CF.getCoon(nurl, Mconf.class);
			Mcf mcf = Mcf.builder().buildApp("node01", "ms-gateway").buildConf("test", "S01", "1.0", "parameter");
			mconf.subscribe(mcf, ParameterEntity.class, new CoonListener<ParameterEntity>() {
				@Override
				public void notify(List<ParameterEntity> list) {
					System.out.println("notify:"+list.toString());
				}
			});
			
			Thread.sleep(3000);
			
			ParameterEntity parameterEntity1 = new ParameterEntity();
			parameterEntity1.setId(System.currentTimeMillis()+"");
			parameterEntity1.setStatus(true);
			parameterEntity1.setOperateTime(new Timestamp(System.currentTimeMillis()));
			parameterEntity1.setKey("channelId");
			parameterEntity1.setTitle("渠道ID");
			parameterEntity1.setType("String");
			Mcf command1 = Mcf.builder().buildApp("node01", "ms-gateway").buildConf("test", "S01", "1.0", "parameter").buildData(parameterEntity1.getId());
			mconf.publish(command1, parameterEntity1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
