package cn.ms.coon.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.Mconf;
import cn.ms.coon.Mlock;
import cn.ms.coon.Mreg;
import cn.ms.coon.support.AbstractCoonFactory;
import cn.ms.neural.NURL;
import cn.ms.neural.extension.Extension;
import cn.ms.neural.extension.ExtensionLoader;

@Extension("redis")
public class RedisCoonFactory extends AbstractCoonFactory {

	private final static Logger logger = LoggerFactory.getLogger(RedisCoonFactory.class);
	
	@Override
	public Mreg createMreg(NURL nurl) {
		logger.info("Is loading mreg center...");

		String key = this.getClass().getAnnotation(Extension.class).value();
		Mreg mreg = ExtensionLoader.getLoader(Mreg.class).getExtension(key);
		mreg.connect(nurl);
		if (!mreg.available()) {
			throw new IllegalStateException("No mreg center available: " + nurl);
		} else {
			logger.info("The mreg center started successed!");
		}

		return mreg;
	}

	@Override
	public Mconf createMconf(NURL nurl) {
		logger.info("Is loading mconf center...");

		String key = this.getClass().getAnnotation(Extension.class).value();
		Mconf mconf = ExtensionLoader.getLoader(Mconf.class).getExtension(key);
		mconf.connect(nurl);
		if (!mconf.available()) {
			throw new IllegalStateException("No mconf center available: " + nurl);
		} else {
			logger.info("The mconf center started successed!");
		}

		return mconf;
	}

	@Override
	public Mlock createMlock(NURL nurl) {
		logger.info("Is loading mlock center...");

		String key = this.getClass().getAnnotation(Extension.class).value();
		Mlock mlock = ExtensionLoader.getLoader(Mlock.class).getExtension(key);
		mlock.connect(nurl);
		if (!mlock.available()) {
			throw new IllegalStateException("No mlock center available: " + nurl);
		} else {
			logger.info("The mlock center started successed!");
		}

		return mlock;
	}
	
}
