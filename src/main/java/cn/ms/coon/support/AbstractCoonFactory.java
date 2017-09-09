package cn.ms.coon.support;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.Mconf;
import cn.ms.coon.Mreg;
import cn.ms.coon.CoonFactory;
import cn.ms.coon.support.common.Consts;
import cn.ms.neural.NURL;

public abstract class AbstractCoonFactory implements CoonFactory {

	private static final Logger logger = LoggerFactory.getLogger(AbstractCoonFactory.class);

    // 注册中心获取过程锁
    private static final ReentrantLock LOCK = new ReentrantLock();
    // 注册中心集合 Map<MregAddress, Mreg>
    private static final Map<String, Mreg> MREGS = new ConcurrentHashMap<String, Mreg>();
    private static final Map<String, Mconf> MCONFS = new ConcurrentHashMap<String, Mconf>();

    public static Collection<Mreg> getMregs() {
        return Collections.unmodifiableCollection(MREGS.values());
    }
    
    public static Collection<Mconf> getMconfs() {
        return Collections.unmodifiableCollection(MCONFS.values());
    }

    @Override
    public Mreg getMreg(NURL nurl) {
    	nurl = nurl.setPath(Mreg.class.getName()).addParameter(Consts.INTERFACE_KEY, Mreg.class.getName());
    	String key = nurl.toServiceString();
    	
        // 锁定注册中心获取过程，保证注册中心单一实例
        LOCK.lock();
        try {
            Mreg mreg = MREGS.get(key);
            if (mreg != null) {
                return mreg;
            }
            mreg = createMreg(nurl);
            if (mreg == null) {
                throw new IllegalStateException("Can not create mreg " + nurl);
            }
            MREGS.put(key, mreg);
            return mreg;
        } finally {
            // 释放锁
            LOCK.unlock();
        }
    }
    
    public static void destroyMregAll() {
        if (logger.isInfoEnabled()) {
        	logger.info("Close all registries " + getMregs());
        }
        
        // 锁定注册中心关闭过程
        LOCK.lock();
        try {
            for (Mreg mreg : getMregs()) {
                try {
                    mreg.destroy();
                } catch (Throwable e) {
                	logger.error(e.getMessage(), e);
                }
            }
            MREGS.clear();
        } finally {
            // 释放锁
            LOCK.unlock();
        }
    }
    
    @Override
    public Mconf getMconf(NURL nurl) {
    	nurl = nurl.setPath(Mreg.class.getName()).addParameter(Consts.INTERFACE_KEY, Mconf.class.getName());
    	String key = nurl.toServiceString();
    	
        // 锁定配置中心获取过程，保证配置中心单一实例
        LOCK.lock();
        try {
        	Mconf mconf = MCONFS.get(key);
            if (mconf != null) {
                return mconf;
            }
            mconf = createMconf(nurl);
            if (mconf == null) {
                throw new IllegalStateException("Can not create mconf " + nurl);
            }
            MCONFS.put(key, mconf);
            return mconf;
        } finally {
            // 释放锁
            LOCK.unlock();
        }
    }
    
    public static void destroyMconfAll() {
        if (logger.isInfoEnabled()) {
        	logger.info("Close all mconfs " + getMconfs());
        }
        
        // 锁定配置中心关闭过程
        LOCK.lock();
        try {
            for (Mconf mconf : getMconfs()) {
                try {
                	mconf.destroy();
                } catch (Throwable e) {
                	logger.error(e.getMessage(), e);
                }
            }
            MCONFS.clear();
        } finally {
            // 释放锁
            LOCK.unlock();
        }
    }

    protected abstract Mreg createMreg(NURL nurl);
    protected abstract Mconf createMconf(NURL nurl);

}