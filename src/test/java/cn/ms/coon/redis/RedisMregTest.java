package cn.ms.coon.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.neural.NURL;

import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import cn.ms.coon.support.CoonListener;

/**
 * RedisRegistryTest
 * 
 * @author lry
 */
public class RedisMregTest {

    String            service     = "cn.ms.test.injvmServie";
    NURL               registryUrl = NURL.valueOf("redis://a:testpass@127.0.0.1:6379/");
    NURL               serviceUrl  = NURL.valueOf("redis://redis/" + service + "?notify=false&methods=test1,test2");
    NURL               consumerUrl = NURL.valueOf("redis://consumer/" + service + "?notify=false&methods=test1,test2");
    RedisMreg registry    = new RedisMreg();
    
    public RedisMregTest() {
    	registry.connect(registryUrl);
	}
    
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
        registry.register(serviceUrl);
    }

    /**
     * Test method for {@link InjvmRegistry#register(java.util.Map)}.
     */
    @Test
    public void testRegister() {
        Set<NURL> registered = null;
        // clear first
        //registered = registry.getRegistered();

        for (int i = 0; i < 2; i++) {
            registry.register(serviceUrl);
            registered = registry.getRegistered();
            assertTrue(registered.contains(serviceUrl));
        }
        // confirm only 1 regist success;
        registered = registry.getRegistered();
        assertEquals(1, registered.size());
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
                // FIXME assertEquals(RedisRegistry.this.service, service);
                args.set(urls.get(0).getParameters());
            }
        });
        assertEquals(serviceUrl.toParameterString(), StringUtils.toQueryString(args.get()));
        Map<String, String> arg = registry.getSubscribed(service);
        assertEquals(subscribearg, StringUtils.toQueryString(arg));*/

    }

}