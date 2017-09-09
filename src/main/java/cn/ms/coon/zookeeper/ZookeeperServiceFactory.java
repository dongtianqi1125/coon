package cn.ms.coon.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.Mconf;
import cn.ms.coon.Mreg;
import cn.ms.coon.support.AbstractMregFactory;
import cn.ms.coon.support.common.Consts;
import cn.ms.coon.zookeeper.transporter.ZkTransporter;
import cn.ms.neural.NURL;
import cn.ms.neural.extension.Extension;
import cn.ms.neural.extension.ExtensionLoader;

@Extension("zookeeper")
public class ZookeeperServiceFactory extends AbstractMregFactory {

	private final static Logger logger = LoggerFactory.getLogger(ZookeeperServiceFactory.class);

	private ZkTransporter transporter;

	public void setZookeeperTransporter(ZkTransporter transporter) {
		this.transporter = transporter;
	}

	@Override
	public Mreg createMreg(NURL nurl) {
		if (transporter == null) {
			String transporter = nurl.getParameter(Consts.TRANSPORTER_KEY, Consts.TRANSPORTER_DEV_VAL);
			this.transporter = ExtensionLoader.getLoader(ZkTransporter.class).getExtension(transporter);
		}
		
		return new ZookeeperMreg(nurl, transporter);
	}
	
	@Override
	public Mconf createMconf(NURL nurl) {
		logger.info("Is loading conf and mconf center...");

		Mconf mconf = new ZookeeperMconf();
		mconf.connect(nurl);
		if (!mconf.available()) {
			throw new IllegalStateException("No mconf center available: " + nurl);
		} else {
			logger.info("The mconf center started successed!");
		}
		
		return mconf;
	}

}