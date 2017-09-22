package io.coon;

import io.neural.NURL;

/**
 * 分布式协调服务
 * 
 * @author lry
 */
public interface CoonService {

	NURL getNurl();

	void connect(NURL nurl);

	boolean available();

	void destroy();

}
