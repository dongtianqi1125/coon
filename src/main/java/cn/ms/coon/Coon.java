package cn.ms.coon;

import cn.ms.neural.NURL;
import cn.ms.neural.extension.ExtensionLoader;

public class Coon {

	public static Mreg getMreg(NURL nurl){
		return ExtensionLoader.getLoader(CoonFactory.class).getExtension(nurl.getProtocol()).getMreg(nurl);
	}
	
	public static Mconf getMconf(NURL nurl){
		return ExtensionLoader.getLoader(CoonFactory.class).getExtension(nurl.getProtocol()).getMconf(nurl);
	}
	
}
