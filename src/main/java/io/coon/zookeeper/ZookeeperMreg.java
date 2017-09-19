package io.coon.zookeeper;

import io.coon.support.Consts;
import io.coon.support.CoonListener;
import io.coon.support.mreg.FailbackMreg;
import io.coon.support.mreg.MregCommon;
import io.coon.support.mreg.exception.MregException;
import io.coon.zookeeper.transporter.ZkTransporter;
import io.coon.zookeeper.transporter.ZkTransporter.ChildListener;
import io.coon.zookeeper.transporter.ZkTransporter.StateListener;
import io.neural.NURL;
import io.neural.extension.Extension;
import io.neural.extension.ExtensionLoader;
import io.neural.util.ConcurrentHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Extension("zookeeper")
public class ZookeeperMreg extends FailbackMreg {

	private static final Logger logger = LoggerFactory.getLogger(ZookeeperMreg.class);

    private final static int DEFAULT_ZOOKEEPER_PORT = 2181;
    private final static String DEFAULT_ROOT = "ms";
    private String root;
    private final Set<String> anyServices = new ConcurrentHashSet<String>();
    private final ConcurrentMap<NURL, ConcurrentMap<CoonListener<NURL>, ChildListener>> zkListeners = new ConcurrentHashMap<NURL, ConcurrentMap<CoonListener<NURL>, ChildListener>>();
    private ZkTransporter transporter;
    
    @Override
    public void connect(NURL nurl) {
        super.connect(nurl);
        if (nurl.isAnyHost()) {
    		throw new IllegalStateException("registry address == null");
    	}
        String group = nurl.getParameter(Consts.GROUP_KEY, DEFAULT_ROOT);
        if (! group.startsWith(Consts.PATH_SEPARATOR)) {
            group = Consts.PATH_SEPARATOR + group;
        }
        
        this.root = group;
        
		String transporter = nurl.getParameter(Consts.TRANSPORTER_KEY, Consts.TRANSPORTER_DEV_VAL);
		this.transporter = ExtensionLoader.getLoader(ZkTransporter.class).getExtension(transporter);
		
		final CountDownLatch countDownLatch = new CountDownLatch(1);
		this.transporter.connect(nurl);
        this.transporter.addStateListener(new StateListener() {
            public void stateChanged(int state) {
            	if (state == RECONNECTED) {
	            	try {
						recover();
					} catch (Exception e) {
						logger.error(e.getMessage(), e);
					}
            	} else if(state == CONNECTED) {
            		countDownLatch.countDown();
            	}
            }
        });
        
        try {
			countDownLatch.await(nurl.getParameter(Consts.TIMEOUT_KEY, Consts.DEFAULT_REGISTRY_CONNECT_TIMEOUT), TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			logger.error("The countDownLatch exception", e);
		}
    }

    @Override
    public boolean available() {
        return transporter.isConnected();
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
        	transporter.close();
        } catch (Exception e) {
            logger.warn("Failed to close zookeeper client " + getNurl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    protected void doRegister(NURL nurl) {
        try {
        	transporter.create(this.toUrlPath(nurl), nurl.getParameter(Consts.DYNAMIC_KEY, true));
        } catch (Throwable e) {
            throw new MregException("Failed to register " + nurl + " to zookeeper " + getNurl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    protected void doUnregister(NURL nurl) {
        try {
        	transporter.delete(this.toUrlPath(nurl));
        } catch (Throwable e) {
            throw new MregException("Failed to unregister " + nurl + " to zookeeper " + getNurl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    protected void doSubscribe(final NURL nurl, final CoonListener<NURL> listener) {
        try {
            if (Consts.ANY_VALUE.equals(nurl.getServiceInterface())) {
                String root = this.toRootPath();
                ConcurrentMap<CoonListener<NURL>, ChildListener> listeners = zkListeners.get(nurl);
                if (listeners == null) {
                    zkListeners.putIfAbsent(nurl, new ConcurrentHashMap<CoonListener<NURL>, ChildListener>());
                    listeners = zkListeners.get(nurl);
                }
                ChildListener zkListener = listeners.get(listener);
                if (zkListener == null) {
                    listeners.putIfAbsent(listener, new ChildListener() {
                        public void childChanged(String parentPath, List<String> currentChilds) {
                            for (String child : currentChilds) {
								child = NURL.decode(child);
                                if (! anyServices.contains(child)) {
                                    anyServices.add(child);
                                    subscribe(nurl.setPath(child).addParameters(Consts.INTERFACE_KEY, child, Consts.CHECK_KEY, String.valueOf(false)), listener);
                                }
                            }
                        }
                    });
                    zkListener = listeners.get(listener);
                }
                transporter.create(root, false);
                List<String> services = transporter.addChildListener(root, zkListener);
                if (services != null && services.size() > 0) {
                    for (String service : services) {
						service = NURL.decode(service);
						anyServices.add(service);
                        this.subscribe(nurl.setPath(service).addParameters(Consts.INTERFACE_KEY, service, Consts.CHECK_KEY, String.valueOf(false)), listener);
                    }
                }
            } else {
                List<NURL> nurls = new ArrayList<NURL>();
                for (String path : toCategoriesPath(nurl)) {
                    ConcurrentMap<CoonListener<NURL>, ChildListener> listeners = zkListeners.get(nurl);
                    if (listeners == null) {
                        zkListeners.putIfAbsent(nurl, new ConcurrentHashMap<CoonListener<NURL>, ChildListener>());
                        listeners = zkListeners.get(nurl);
                    }
                    ChildListener zkListener = listeners.get(listener);
                    if (zkListener == null) {
                        listeners.putIfAbsent(listener, new ChildListener() {
                            public void childChanged(String parentPath, List<String> currentChilds) {
                            	ZookeeperMreg.this.notify(nurl, listener, toUrlsWithEmpty(nurl, parentPath, currentChilds));
                            }
                        });
                        zkListener = listeners.get(listener);
                    }
                    transporter.create(path, false);
                    List<String> children = transporter.addChildListener(path, zkListener);
                    if (children != null) {
                    	nurls.addAll(this.toUrlsWithEmpty(nurl, path, children));
                    }
                }
                this.notify(nurl, listener, nurls);
            }
        } catch (Throwable e) {
            throw new MregException("Failed to subscribe " + nurl + " to zookeeper " + getNurl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    protected void doUnsubscribe(NURL nurl, CoonListener<NURL> listener) {
        ConcurrentMap<CoonListener<NURL>, ChildListener> listeners = zkListeners.get(nurl);
        if (listeners != null) {
            ChildListener zkListener = listeners.get(listener);
            if (zkListener != null) {
            	transporter.removeChildListener(toUrlPath(nurl), zkListener);
            }
        }
    }

    @Override
    public List<NURL> lookup(NURL nurl) {
        if (nurl == null) {
            throw new IllegalArgumentException("lookup nurl == null");
        }
        
        try {
            List<String> providers = new ArrayList<String>();
            for (String path : this.toCategoriesPath(nurl)) {
                List<String> children = transporter.getChildren(path);
                if (children != null) {
                    providers.addAll(children);
                }
            }
            
            return this.toUrlsWithoutEmpty(nurl, providers);
        } catch (Throwable e) {
            throw new MregException("Failed to lookup " + nurl + " from zookeeper " + getNurl() + ", cause: " + e.getMessage(), e);
        }
    }
    
    private String toRootDir() {
        if (root.equals(Consts.PATH_SEPARATOR)) {
            return root;
        }
        return root + Consts.PATH_SEPARATOR;
    }
    
    private String toRootPath() {
        return root;
    }
    
    private String toServicePath(NURL nurl) {
        String name = nurl.getServiceInterface();
        if (Consts.ANY_VALUE.equals(name)) {
            return this.toRootPath();
        }
        
        return this.toRootDir() + NURL.encode(name);
    }

    private String[] toCategoriesPath(NURL nurl) {
        String[] categroies;
        if (Consts.ANY_VALUE.equals(nurl.getParameter(Consts.CATEGORY_KEY))) {
            categroies = new String[] {Consts.PROVIDERS_CATEGORY, Consts.CONSUMERS_CATEGORY, Consts.ROUTERS_CATEGORY, Consts.CONFIGURATORS_CATEGORY};
        } else {
            categroies = nurl.getParameter(Consts.CATEGORY_KEY, new String[] {Consts.DEFAULT_CATEGORY});
        }
        String[] paths = new String[categroies.length];
        for (int i = 0; i < categroies.length; i ++) {
            paths[i] = this.toServicePath(nurl) + Consts.PATH_SEPARATOR + categroies[i];
        }
        
        return paths;
    }

    private String toCategoryPath(NURL nurl) {
        return this.toServicePath(nurl) + Consts.PATH_SEPARATOR + nurl.getParameter(Consts.CATEGORY_KEY, Consts.DEFAULT_CATEGORY);
    }

    private String toUrlPath(NURL nurl) {
        return this.toCategoryPath(nurl) + Consts.PATH_SEPARATOR + NURL.encode(nurl.toFullString());
    }
    
    private List<NURL> toUrlsWithoutEmpty(NURL consumer, List<String> providers) {
    	List<NURL> nurls = new ArrayList<NURL>();
        if (providers != null && providers.size() > 0) {
            for (String provider : providers) {
                provider = NURL.decode(provider);
                if (provider.contains("://")) {
                	NURL nurl = NURL.valueOf(provider);
                    if (MregCommon.isMatch(consumer, nurl)) {
                        nurls.add(nurl);
                    }
                }
            }
        }
        
        return nurls;
    }

    private List<NURL> toUrlsWithEmpty(NURL consumer, String path, List<String> providers) {
        List<NURL> nurls = this.toUrlsWithoutEmpty(consumer, providers);
        if (nurls.isEmpty()) {
        	int i = path.lastIndexOf('/');
        	String category = i < 0 ? path : path.substring(i + 1);
        	NURL empty = consumer.setProtocol(Consts.EMPTY_PROTOCOL).addParameter(Consts.CATEGORY_KEY, category);
            nurls.add(empty);
        }
        
        return nurls;
    }

    public static String appendDefaultPort(String address) {
        if (address != null && address.length() > 0) {
            int i = address.indexOf(':');
            if (i < 0) {
                return address + ":" + DEFAULT_ZOOKEEPER_PORT;
            } else if (Integer.parseInt(address.substring(i + 1)) == 0) {
                return address.substring(0, i + 1) + DEFAULT_ZOOKEEPER_PORT;
            }
        }
        
        return address;
    }

}