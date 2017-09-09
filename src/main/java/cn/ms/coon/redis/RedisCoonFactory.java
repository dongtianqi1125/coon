package cn.ms.coon.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.Mconf;
import cn.ms.coon.Mreg;
import cn.ms.coon.support.AbstractCoonFactory;
import cn.ms.neural.NURL;
import cn.ms.neural.extension.Extension;

@Extension("redis")
public class RedisCoonFactory extends AbstractCoonFactory {

	private final static Logger logger = LoggerFactory.getLogger(RedisCoonFactory.class);
	
	@Override
	public Mreg createMreg(NURL nurl) {
		return new RedisMreg(nurl);
	}

	@Override
	public Mconf createMconf(NURL nurl) {
		logger.info("Is loading conf and mconf center...");

		Mconf mconf = new RedisMconf();
		mconf.connect(nurl);
		if (!mconf.available()) {
			throw new IllegalStateException("No mconf center available: " + nurl);
		} else {
			logger.info("The mconf center started successed!");
		}
		
		return mconf;
	}
	
}
