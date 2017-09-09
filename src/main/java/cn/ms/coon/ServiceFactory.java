package cn.ms.coon;

import cn.ms.neural.NURL;
import cn.ms.neural.extension.NSPI;

@NSPI("zookeeper")
public interface ServiceFactory {

    /**
     * 连接注册中心.
     * 
     * 连接注册中心需处理契约：<br>
     * 1. 当设置check=false时表示不检查连接，否则在连接不上时抛出异常。<br>
     * 2. 支持NURL上的username:password权限认证。<br>
     * 3. 支持backup=1.2.3.4:2181,1.2.3.5:2181备选注册中心集群地址。<br>
     * 4. 支持file=registry.cache本地磁盘文件缓存。<br>
     * 5. 支持timeout=1000请求超时设置。<br>
     * 6. 支持session=60000会话超时或过期设置。<br>
     * 
     * @param nurl 注册中心地址，不允许为空
     * @return 注册中心引用，总不返回空
     */
    Mreg getMreg(NURL nurl);
    
    Mconf getMconf(NURL nurl);

}