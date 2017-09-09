package cn.ms.coon.redis;

import java.util.List;

import cn.ms.coon.CoonListener;
import cn.ms.coon.support.mconf.AbstractMconf;
import cn.ms.neural.NURL;

/**
 * The base of Redis Mconf.
 * 
 * @author lry
 */
public class RedisMconf extends AbstractMconf {

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
	public boolean available() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
	}
	
}
