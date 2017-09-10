package cn.ms.coon;

import cn.ms.neural.NURL;
import cn.ms.neural.extension.NSPI;

/**
 * 分布式协调服务
 * 
 * @author lry
 */
@NSPI("zookeeper")
public interface CoonFactory {

	/**
	 * 获取分布式协调服务
	 * 
	 * @param nurl
	 * @param cls {@link Mreg}/{@link Mconf}/{@link Mlock}
	 * @return
	 */
	<T> T getCoon(NURL nurl, Class<T> cls);

}