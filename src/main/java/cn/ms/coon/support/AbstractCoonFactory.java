package cn.ms.coon.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.Coon;
import cn.ms.coon.CoonFactory;
import cn.ms.coon.Mconf;
import cn.ms.coon.Mlock;
import cn.ms.coon.Mreg;
import cn.ms.neural.NURL;

public abstract class AbstractCoonFactory implements CoonFactory {

	private static final Logger logger = LoggerFactory.getLogger(AbstractCoonFactory.class);

	private static final ReentrantLock LOCK = new ReentrantLock();
	private static final Map<String, Coon> COON_MAP = new ConcurrentHashMap<String, Coon>();

	public static <T> Collection<Coon> getCoons() {
		return Collections.unmodifiableCollection(COON_MAP.values());
	}

	@SuppressWarnings("unchecked")
	public static <T> Collection<T> getCoons(Class<T> cls) {
		List<T> list = new ArrayList<T>();
		for (Coon coon : COON_MAP.values()) {
			if (coon.getClass().getName().equals(cls.getName())) {
				list.add((T) coon);
			}
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getCoon(NURL nurl, Class<T> cls) {
		nurl = nurl.setPath(Mreg.class.getName()).addParameter(Consts.INTERFACE_KEY, Mreg.class.getName());
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
			} else if (Mlock.class.getName().equals(cls.getName())) {
				coon = createMlock(nurl);
			}

			if (coon == null) {
				throw new IllegalStateException("Can not create coon " + nurl);
			}

			COON_MAP.put(key, coon);

			return (T) coon;
		} finally {
			LOCK.unlock();
		}
	}

	public static void destroyMregAll() {
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

	protected abstract Mreg createMreg(NURL nurl);
	protected abstract Mconf createMconf(NURL nurl);
	protected abstract Mlock createMlock(NURL nurl);

}