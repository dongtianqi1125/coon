package cn.ms.coon;

import org.junit.Assert;
import org.junit.Test;

import cn.ms.coon.ServiceFactory;
import cn.ms.coon.redis.RedisServiceFactory;
import cn.ms.coon.zookeeper.ZookeeperServiceFactory;
import cn.ms.neural.extension.ExtensionLoader;

public class MregFactoryTest {

	@Test
	public void redisRegistryFactory() {
		ServiceFactory serviceFactory = ExtensionLoader.getLoader(ServiceFactory.class).getExtension("redis");
		Assert.assertTrue(serviceFactory instanceof RedisServiceFactory);
	}

	@Test
	public void zookeeperRegistryFactory() {
		ServiceFactory serviceFactory = ExtensionLoader.getLoader(ServiceFactory.class).getExtension("zookeeper");
		Assert.assertTrue(serviceFactory instanceof ZookeeperServiceFactory);
	}

}
