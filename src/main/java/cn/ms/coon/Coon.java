package cn.ms.coon;

import cn.ms.neural.NURL;

public interface Coon {

	NURL getNurl();

	boolean available();

	void destroy();

}
