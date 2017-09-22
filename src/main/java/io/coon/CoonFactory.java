package io.coon;

import io.coon.api.Mconf;
import io.coon.api.Mreg;
import io.coon.support.Consts;
import io.neural.NURL;
import io.neural.extension.ExtensionLoader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式协调服务
 * 
 * @author lry
 */
public enum CoonFactory {

	CF;
	
	private static final Logger logger = LoggerFactory.getLogger(CoonFactory.class);

	private final ReentrantLock LOCK = new ReentrantLock();
	private final Map<String, Coon> COON_MAP = new ConcurrentHashMap<String, Coon>();

	public <T> Collection<Coon> getCoons() {
		return Collections.unmodifiableCollection(COON_MAP.values());
	}

	@SuppressWarnings("unchecked")
	public <T> Collection<T> getCoons(Class<T> cls) {
		List<T> list = new ArrayList<T>();
		for (Coon coon : COON_MAP.values()) {
			if (coon.getClass().getName().equals(cls.getName())) {
				list.add((T) coon);
			}
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	public <T> T getCoon(NURL nurl, Class<T> cls) {
		nurl = nurl.setPath(cls.getName()).addParameter(Consts.INTERFACE_KEY, cls.getName());
		String key = nurl.toServiceString();

		LOCK.lock();
		try {
			Coon coon = COON_MAP.get(key);
			if (coon != null) {
				return (T) coon;
			}

			if (Mreg.class.getName().equals(cls.getName())) {
				coon = createMreg(nurl);
			} else if (Mconf.class.getName().equals(cls.getName())) {
				coon = createMconf(nurl);
			}

			if (coon == null) {
				throw new IllegalStateException("Can not create coon " + nurl);
			} else {
				coon.connect(nurl);
			}

			COON_MAP.put(key, coon);

			return (T) coon;
		} finally {
			LOCK.unlock();
		}
	}

	public void destroyMregAll() {
		if (logger.isInfoEnabled()) {
			logger.info("Close all coons " + getCoons());
		}

		LOCK.lock();
		try {
			for (Coon coon : getCoons()) {
				try {
					coon.destroy();
				} catch (Throwable e) {
					logger.error(e.getMessage(), e);
				}
			}
			COON_MAP.clear();
		} finally {
			LOCK.unlock();
		}
	}

	private Mreg createMreg(NURL nurl) {
		logger.info("Is loading mreg center...");
		Mreg mreg = ExtensionLoader.getLoader(Mreg.class).getExtension(nurl.getProtocol());
		mreg.connect(nurl);
		if (!mreg.available()) {
			throw new IllegalStateException("No mreg center available: " + nurl);
		} else {
			logger.info("The mreg center started successed!");
		}

		return mreg;
	}

	private Mconf createMconf(NURL nurl) {
		logger.info("Is loading mconf center...");
		Mconf mconf = ExtensionLoader.getLoader(Mconf.class).getExtension(nurl.getProtocol());
		mconf.connect(nurl);
		if (!mconf.available()) {
			throw new IllegalStateException("No mconf center available: " + nurl);
		} else {
			logger.info("The mconf center started successed!");
		}

		return mconf;
	}

}