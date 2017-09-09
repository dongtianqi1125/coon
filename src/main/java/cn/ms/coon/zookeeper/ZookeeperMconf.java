package cn.ms.coon.zookeeper;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.CoonListener;
import cn.ms.coon.support.Consts;
import cn.ms.coon.support.mconf.AbstractMconf;
import cn.ms.coon.zookeeper.transporter.ZkTransporter;
import cn.ms.neural.NURL;
import cn.ms.neural.extension.ExtensionLoader;
import cn.ms.neural.extension.NSPI;

/**
 * The base of Zookeeper Mconf.
 * 
 * @author lry
 */
public class ZookeeperMconf extends AbstractMconf {

	private static final Logger logger = LoggerFactory.getLogger(ZookeeperMconf.class);

	private ZkTransporter transporter;
	
	@Override
	public void connect(NURL nurl) {
		super.connect(nurl);
		NSPI nspi = ZkTransporter.class.getAnnotation(NSPI.class);
		String transporter = nurl.getParameter(Consts.TRANSPORTER_KEY, nspi.value());
		this.transporter = ExtensionLoader.getLoader(ZkTransporter.class).getExtension(transporter);
	}
	
	@Override
	public boolean available() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void publish(NURL nurl, Object obj) {
		// TODO Auto-generated method stub

	}

	@Override
	public void unpublish(NURL nurl, Object obj) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void subscribe(NURL nurl, CoonListener<T> listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void unsubscribe(NURL nurl, CoonListener<T> listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> List<T> lookup(NURL nurl) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub

	}

}
