package cn.ms.coon.support.mreg;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.service.Mreg;
import cn.ms.coon.support.Consts;
import cn.ms.coon.support.CoonListener;
import cn.ms.coon.support.NamedThreadFactory;
import cn.ms.neural.NURL;
import cn.ms.neural.util.micro.ConcurrentHashSet;

public abstract class AbstractMreg implements Mreg {

	private static final Logger logger = LoggerFactory.getLogger(AbstractMreg.class);

    // NURL地址分隔符，用于文件缓存中，服务提供者EURL分隔
    private static final char NURL_SEPARATOR = ' ';
    // NURL地址分隔正则表达式，用于解析文件缓存中服务提供者EURL列表
    private static final String NURL_SPLIT = "\\s+";
    private NURL mregNurl;
    // 本地磁盘缓存文件
    private File file;
    // 本地磁盘缓存，其中特殊的key值.registies记录注册中心列表，其它均为notified服务提供者列表
    private final Properties properties = new Properties();
    // 文件缓存定时写入
    private final ExecutorService mregCacheExecutor = Executors.newFixedThreadPool(1, new NamedThreadFactory("LocalSaveMregCache", true));
    //是否是同步保存文件
    private boolean syncSaveFile ;
    private final AtomicLong lastCacheChanged = new AtomicLong();
    private final Set<NURL> registered = new ConcurrentHashSet<NURL>();
    private final ConcurrentMap<NURL, Set<CoonListener<NURL>>> subscribed = new ConcurrentHashMap<NURL, Set<CoonListener<NURL>>>();
    private final ConcurrentMap<NURL, Map<String, List<NURL>>> notified = new ConcurrentHashMap<NURL, Map<String, List<NURL>>>();

    @Override
    public void connect(NURL nurl) {
    	this.setNurl(nurl);
    	
        // 启动文件保存定时器
        syncSaveFile = nurl.getParameter(Consts.REGISTRY_FILESAVE_SYNC_KEY, false);
        String filename = nurl.getParameter(Consts.FILE_KEY, System.getProperty("user.home") + "/.mreg/mreg-" + nurl.getHost() + ".cache");
        File file = null;
        if (!MregCommon.isEmpty2(filename)) {
            file = new File(filename);
            if(! file.exists() && file.getParentFile() != null && ! file.getParentFile().exists()){
                if(! file.getParentFile().mkdirs()){
                    throw new IllegalArgumentException("Invalid mreg store file " + file + ", cause: Failed to create directory " + file.getParentFile() + "!");
                }
            }
        }
        this.file = file;
        loadProperties();
        notify(nurl.getBackupUrls());
    }

    protected void setNurl(NURL nurl) {
        if (nurl == null) {
            throw new IllegalArgumentException("mreg nurl == null");
        }
        this.mregNurl = nurl;
    }

    @Override
    public NURL getNurl() {
        return mregNurl;
    }

    public Set<NURL> getRegistered() {
        return registered;
    }

    public Map<NURL, Set<CoonListener<NURL>>> getSubscribed() {
        return subscribed;
    }

    public Map<NURL, Map<String, List<NURL>>> getNotified() {
        return notified;
    }

    public File getCacheFile() {
        return file;
    }

    public Properties getCacheProperties() {
        return properties;
    }

    public AtomicLong getLastCacheChanged(){
        return lastCacheChanged;
    }

    private class SaveProperties implements Runnable{
        private long version;
        private SaveProperties(long version){
            this.version = version;
        }
        public void run() {
            doSaveProperties(version);
        }
    }
    
    public void doSaveProperties(long version) {
        if(version < lastCacheChanged.get()){
            return;
        }
        if (file == null) {
            return;
        }
        Properties newProperties = new Properties();
        // 保存之前先读取一遍，防止多个注册中心之间冲突
        InputStream in = null;
        try {
            if (file.exists()) {
                in = new FileInputStream(file);
                newProperties.load(in);
            }
        } catch (Throwable e) {
            logger.warn("Failed to load mreg store file, cause: " + e.getMessage(), e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        }     
        
        // 保存
        try {
			newProperties.putAll(properties);
            File lockfile = new File(file.getAbsolutePath() + ".lock");
            if (!lockfile.exists()) {
            	lockfile.createNewFile();
            }
            RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");
            try {
                FileChannel channel = raf.getChannel();
                try {
                    FileLock lock = channel.tryLock();
                	if (lock == null) {
                        throw new IOException("Can not lock the mreg cache file " + file.getAbsolutePath() + ", ignore and retry later, maybe multi java process use the file, please config: dubbo.mreg.file=xxx.properties");
                    }
                	// 保存
                    try {
                    	if (! file.exists()) {
                            file.createNewFile();
                        }
                        FileOutputStream outputFile = new FileOutputStream(file);  
                        try {
                            newProperties.store(outputFile, "Ms Registry Cache");
                        } finally {
                        	outputFile.close();
                        }
                    } finally {
                    	lock.release();
                    }
                } finally {
                    channel.close();
                }
            } finally {
                raf.close();
            }
        } catch (Throwable e) {
            if (version < lastCacheChanged.get()) {
                return;
            } else {
                mregCacheExecutor.execute(new SaveProperties(lastCacheChanged.incrementAndGet()));
            }
            logger.warn("Failed to save mreg store file, cause: " + e.getMessage(), e);
        }
    }

    private void loadProperties() {
        if (file != null && file.exists()) {
            InputStream in = null;
            try {
                in = new FileInputStream(file);
                properties.load(in);
                if (logger.isInfoEnabled()) {
                    logger.info("Load mreg store file " + file + ", data: " + properties);
                }
            } catch (Throwable e) {
                logger.warn("Failed to load mreg store file " + file, e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        }
    }

    public List<NURL> getCacheUrls(NURL nurl) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key != null && key.length() > 0 && key.equals(nurl.getServiceKey())
                    && (Character.isLetter(key.charAt(0)) || key.charAt(0) == '_')
                    && value != null && value.length() > 0) {
                String[] arr = value.trim().split(NURL_SPLIT);
                List<NURL> nurls = new ArrayList<NURL>();
                for (String u : arr) {
                    nurls.add(NURL.valueOf(u));
                }
                return nurls;
            }
        }
        return null;
    }

    @Override
    public List<NURL> lookup(NURL nurl) {
        List<NURL> result = new ArrayList<NURL>();
        Map<String, List<NURL>> notifiedUrls = getNotified().get(nurl);
        if (notifiedUrls != null && notifiedUrls.size() > 0) {
            for (List<NURL> nurls : notifiedUrls.values()) {
                for (NURL u : nurls) {
                    if (! Consts.EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        } else {
            final AtomicReference<List<NURL>> reference = new AtomicReference<List<NURL>>();
            CoonListener<NURL> listener = new CoonListener<NURL>() {
                public void notify(List<NURL> nurls) {
                    reference.set(nurls);
                }
            };
            subscribe(nurl, listener); // 订阅逻辑保证第一次notify后再返回
            List<NURL> nurls = reference.get();
            if (nurls != null && nurls.size() > 0) {
                for (NURL u : nurls) {
                    if (! Consts.EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void register(NURL nurl) {
        if (nurl == null) {
            throw new IllegalArgumentException("register nurl == null");
        }
        if (logger.isInfoEnabled()){
            logger.info("Register: " + nurl);
        }
        registered.add(nurl);
    }

    @Override
    public void unregister(NURL nurl) {
        if (nurl == null) {
            throw new IllegalArgumentException("unregister nurl == null");
        }
        if (logger.isInfoEnabled()){
            logger.info("Unregister: " + nurl);
        }
        registered.remove(nurl);
    }

    @Override
    public void subscribe(NURL nurl, CoonListener<NURL> listener) {
        if (nurl == null) {
            throw new IllegalArgumentException("subscribe nurl == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("subscribe listener == null");
        }
        if (logger.isInfoEnabled()){
            logger.info("Subscribe: " + nurl);
        }
        Set<CoonListener<NURL>> listeners = subscribed.get(nurl);
        if (listeners == null) {
            subscribed.putIfAbsent(nurl, new ConcurrentHashSet<CoonListener<NURL>>());
            listeners = subscribed.get(nurl);
        }
        listeners.add(listener);
    }

    @Override
    public void unsubscribe(NURL nurl, CoonListener<NURL> listener) {
        if (nurl == null) {
            throw new IllegalArgumentException("unsubscribe nurl == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("unsubscribe listener == null");
        }
        if (logger.isInfoEnabled()){
            logger.info("Unsubscribe: " + nurl);
        }
        Set<CoonListener<NURL>> listeners = subscribed.get(nurl);
        if (listeners != null) {
            listeners.remove(listener);
        }
    }

    protected void recover() throws Exception {
        // register
        Set<NURL> recoverRegistered = new HashSet<NURL>(getRegistered());
        if (! recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register nurl " + recoverRegistered);
            }
            for (NURL nurl : recoverRegistered) {
                register(nurl);
            }
        }
        // subscribe
        Map<NURL, Set<CoonListener<NURL>>> recoverSubscribed = new HashMap<NURL, Set<CoonListener<NURL>>>(getSubscribed());
        if (! recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe nurl " + recoverSubscribed.keySet());
            }
            for (Map.Entry<NURL, Set<CoonListener<NURL>>> entry : recoverSubscribed.entrySet()) {
            	NURL nurl = entry.getKey();
                for (CoonListener<NURL> listener : entry.getValue()) {
                    subscribe(nurl, listener);
                }
            }
        }
    }

    protected static List<NURL> filterEmpty(NURL nurl, List<NURL> nurls) {
        if (nurls == null || nurls.size() == 0) {
            List<NURL> result = new ArrayList<NURL>(1);
            result.add(nurl.setProtocol(Consts.EMPTY_PROTOCOL));
            return result;
        }
        return nurls;
    }

    protected void notify(List<NURL> nurls) {
        if(nurls == null || nurls.isEmpty()) return;
        
        for (Map.Entry<NURL, Set<CoonListener<NURL>>> entry : getSubscribed().entrySet()) {
        	NURL nurl = entry.getKey();
            
            if(! MregCommon.isMatch(nurl, nurls.get(0))) {
                continue;
            }
            
            Set<CoonListener<NURL>> listeners = entry.getValue();
            if (listeners != null) {
                for (CoonListener<NURL> listener : listeners) {
                    try {
                        notify(nurl, listener, filterEmpty(nurl, nurls));
                    } catch (Throwable t) {
                        logger.error("Failed to notify mreg event, nurls: " +  nurls + ", cause: " + t.getMessage(), t);
                    }
                }
            }
        }
    }

    protected void notify(NURL nurl, CoonListener<NURL> listener, List<NURL> nurls) {
        if (nurl == null) {
            throw new IllegalArgumentException("notify nurl == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        if ((nurls == null || nurls.size() == 0) 
                && ! Consts.ANY_VALUE.equals(nurl.getServiceInterface())) {
            logger.warn("Ignore empty notify nurls for subscribe nurl " + nurl);
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Notify nurls for subscribe nurl " + nurl + ", nurls: " + nurls);
        }
        Map<String, List<NURL>> result = new HashMap<String, List<NURL>>();
        for (NURL u : nurls) {
            if (MregCommon.isMatch(nurl, u)) {
            	String category = u.getParameter(Consts.CATEGORY_KEY, Consts.DEFAULT_CATEGORY);
            	List<NURL> categoryList = result.get(category);
            	if (categoryList == null) {
            		categoryList = new ArrayList<NURL>();
            		result.put(category, categoryList);
            	}
            	categoryList.add(u);
            }
        }
        if (result.size() == 0) {
            return;
        }
        Map<String, List<NURL>> categoryNotified = notified.get(nurl);
        if (categoryNotified == null) {
            notified.putIfAbsent(nurl, new ConcurrentHashMap<String, List<NURL>>());
            categoryNotified = notified.get(nurl);
        }
        for (Map.Entry<String, List<NURL>> entry : result.entrySet()) {
            String category = entry.getKey();
            List<NURL> categoryList = entry.getValue();
            categoryNotified.put(category, categoryList);
            saveProperties(nurl);
            listener.notify(categoryList);
        }
    }

    private void saveProperties(NURL nurl) {
        if (file == null) {
            return;
        }
        
        try {
            StringBuilder buf = new StringBuilder();
            Map<String, List<NURL>> categoryNotified = notified.get(nurl);
            if (categoryNotified != null) {
                for (List<NURL> us : categoryNotified.values()) {
                    for (NURL u : us) {
                        if (buf.length() > 0) {
                            buf.append(NURL_SEPARATOR);
                        }
                        buf.append(u.toFullString());
                    }
                }
            }
            properties.setProperty(nurl.getServiceKey(), buf.toString());
            long version = lastCacheChanged.incrementAndGet();
            if (syncSaveFile) {
                doSaveProperties(version);
            } else {
                mregCacheExecutor.execute(new SaveProperties(version));
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    @Override
    public void destroy() {
        if (logger.isInfoEnabled()){
            logger.info("Destroy mreg:" + getNurl());
        }
        Set<NURL> destroyRegistered = new HashSet<NURL>(getRegistered());
        if (! destroyRegistered.isEmpty()) {
            for (NURL nurl : new HashSet<NURL>(getRegistered())) {
                if (nurl.getParameter(Consts.DYNAMIC_KEY, true)) {
                    try {
                        unregister(nurl);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unregister nurl " + nurl);
                        }
                    } catch (Throwable t) {
                        logger.warn("Failed to unregister nurl " + nurl + " to mreg " + getNurl() + " on destroy, cause: " + t.getMessage(), t);
                    }
                }
            }
        }
        Map<NURL, Set<CoonListener<NURL>>> destroySubscribed = new HashMap<NURL, Set<CoonListener<NURL>>>(getSubscribed());
        if (! destroySubscribed.isEmpty()) {
            for (Map.Entry<NURL, Set<CoonListener<NURL>>> entry : destroySubscribed.entrySet()) {
            	NURL nurl = entry.getKey();
                for (CoonListener<NURL> listener : entry.getValue()) {
                    try {
                        unsubscribe(nurl, listener);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unsubscribe nurl " + nurl);
                        }
                    } catch (Throwable t) {
                        logger.warn("Failed to unsubscribe nurl " + nurl + " to mreg " + getNurl() + " on destroy, cause: " +t.getMessage(), t);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return getNurl().toString();
    }

}