package cn.ms.coon;

import cn.ms.neural.NURL;
import cn.ms.neural.extension.ExtensionLoader;

public class Emsf {

	public static Mreg getMreg(NURL nurl){
		return ExtensionLoader.getLoader(ServiceFactory.class).getExtension(nurl.getProtocol()).getMreg(nurl);
	}
	
	public static Mconf getMconf(NURL nurl){
		return ExtensionLoader.getLoader(ServiceFactory.class).getExtension(nurl.getProtocol()).getMconf(nurl);
	}
	
}
