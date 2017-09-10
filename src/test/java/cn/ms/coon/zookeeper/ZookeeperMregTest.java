package cn.ms.coon.zookeeper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import cn.ms.coon.support.CoonListener;
import cn.ms.neural.NURL;

/**
 * ZookeeperRegistryTest
 * 
 * @author lry
 */
public class ZookeeperMregTest {

    String            service     = "cn.ms.test.injvmServie";
    NURL               registryUrl = NURL.valueOf("zookeeper://239.255.255.255/");
    NURL               serviceUrl  = NURL.valueOf("zookeeper://zookeeper/" + service + "?notify=false&methods=test1,test2");
    NURL               consumerUrl = NURL.valueOf("zookeeper://consumer/" + service + "?notify=false&methods=test1,test2");
    // ZookeeperRegistry registry    = new ZookeeperRegistry(registryUrl);

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        //registry.register(service, serviceUrl);
    }

    /*@Test(expected = IllegalStateException.class)
    public void testUrlerror() {
        EURL errorUrl = EURL.valueOf("zookeeper://zookeeper/");
        new ZookeeperRegistry(errorUrl);
    }*/
    
    @Test
    public void testDefaultPort() {
        Assert.assertEquals("10.20.153.10:2181", ZookeeperMreg.appendDefaultPort("10.20.153.10:0"));
        Assert.assertEquals("10.20.153.10:2181", ZookeeperMreg.appendDefaultPort("10.20.153.10"));
    }

    /**
     * Test method for {@link InjvmRegistry#register(java.util.Map)}.
     */
    @Test
    public void testRegister() {
        /*List<EURL> registered = null;
        // clear first
        registered = registry.getRegistered(service);

        for (int i = 0; i < 2; i++) {
            registry.register(service, serviceUrl);
            registered = registry.getRegistered(service);
            assertTrue(registered.contains(serviceUrl));
        }
        // confirm only 1 regist success;
        registered = registry.getRegistered(service);
        assertEquals(1, registered.size());*/
    }

    /**
     * Test method for
     * {@link InjvmRegistry#subscribe(java.util.Map, CoonListener)}
     * .
     */
    @Test
    public void testSubscribe() {
        /*final String subscribearg = "arg1=1&arg2=2";
        // verify lisener.
        final AtomicReference<Map<String, String>> args = new AtomicReference<Map<String, String>>();
        registry.subscribe(service, new EURL("http", NetUtils.getLocalHost(), 0, StringUtils.parseQueryString(subscribearg)), new NotifyListener() {

            public void notify(List<EURL> urls) {
                // FIXME assertEquals(ZookeeperRegistry.this.service, service);
                args.set(urls.get(0).getParameters());
            }
        });
        assertEquals(serviceUrl.toParameterString(), StringUtils.toQueryString(args.get()));
        Map<String, String> arg = registry.getSubscribed(service);
        assertEquals(subscribearg, StringUtils.toQueryString(arg));*/
    }

}