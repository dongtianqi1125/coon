package cn.ms.coon;

import java.util.List;

import cn.ms.coon.support.CoonListener;
import cn.ms.neural.NURL;
import cn.ms.neural.extension.NSPI;

/**
 * The MicroService Registry Center.<br>
 * <br>
 * 连接注册中心需处理契约：<br>
 * 1. 当设置check=false时表示不检查连接，否则在连接不上时抛出异常。<br>
 * 2. 支持NURL上的username:password权限认证。<br>
 * 3. 支持backup=1.2.3.4:2181,1.2.3.5:2181备选注册中心集群地址。<br>
 * 4. 支持file=registry.cache本地磁盘文件缓存。<br>
 * 5. 支持timeout=1000请求超时设置。<br>
 * 6. 支持session=60000会话超时或过期设置。<br>
 * 
 * @author lry
 */
@NSPI("zookeeper")
public interface Mreg extends Coon {
	
    /**
     * 注册数据，比如：提供者地址，消费者地址，路由规则，覆盖规则，等数据。
     * 
     * 注册需处理契约：<br>
     * 1. 当NURL设置了check=false时，注册失败后不报错，在后台定时重试，否则抛出异常。<br>
     * 2. 当NURL设置了dynamic=false参数，则需持久存储，否则，当注册者出现断电等情况异常退出时，需自动删除。<br>
     * 3. 当NURL设置了category=routers时，表示分类存储，缺省类别为providers，可按分类部分通知数据。<br>
     * 4. 当注册中心重启，网络抖动，不能丢失数据，包括断线自动删除数据。<br>
     * 5. 允许URI相同但参数不同的NURL并存，不能覆盖。<br>
     * 
     * @param nurl 注册信息，不允许为空，如：tcp://10.20.153.10/cn.ms.test.TestService?version=1.0.0&application=kylin
     */
    void register(NURL nurl);

    /**
     * 取消注册.
     * 
     * 取消注册需处理契约：<br>
     * 1. 如果是dynamic=false的持久存储数据，找不到注册数据，则抛IllegalStateException，否则忽略。<br>
     * 2. 按全NURL匹配取消注册。<br>
     * 
     * @param nurl 注册信息，不允许为空，如：tcp://10.20.153.10/cn.ms.test.TestService?version=1.0.0&application=kylin
     */
    void unregister(NURL nurl);

    /**
     * 订阅符合条件的已注册数据，当有注册数据变更时自动推送.
     * 
     * 订阅需处理契约：<br>
     * 1. 当NURL设置了check=false时，订阅失败后不报错，在后台定时重试。<br>
     * 2. 当NURL设置了category=routers，只通知指定分类的数据，多个分类用逗号分隔，并允许星号通配，表示订阅所有分类数据。<br>
     * 3. 允许以interface,group,version,classifier作为条件查询，如：interface=cn.ms.test.TestService&version=1.0.0<br>
     * 4. 并且查询条件允许星号通配，订阅所有接口的所有分组的所有版本，或：interface=*&group=*&version=*&classifier=*<br>
     * 5. 当注册中心重启，网络抖动，需自动恢复订阅请求。<br>
     * 6. 允许URI相同但参数不同的URL并存，不能覆盖。<br>
     * 7. 必须阻塞订阅过程，等第一次通知完后再返回。<br>
     * 
     * 
     * 当收到服务变更通知时触发。
     * 通知需处理契约：<br>
     * 1. 总是以服务接口和数据类型为维度全量通知，即不会通知一个服务的同类型的部分数据，用户不需要对比上一次通知结果。<br>
     * 2. 订阅时的第一次通知，必须是一个服务的所有类型数据的全量通知。<br>
     * 3. 中途变更时，允许不同类型的数据分开通知，比如：providers, consumers, routers, overrides，允许只通知其中一种类型，但该类型的数据必须是全量的，不是增量的。<br>
     * 4. 如果一种类型的数据为空，需通知一个empty协议并带category参数的标识性URL数据。<br>
     * 5. 通知者(即注册中心实现)需保证通知的顺序，比如：单线程推送，队列串行化，带版本对比。<br>
     * 
     * @param nurl 订阅条件，不允许为空，如：consumer://10.20.153.10/cn.ms.test.TestService?version=1.0.0&application=kylin
     * @param listener 变更事件监听器，不允许为空
     */
    void subscribe(NURL nurl, CoonListener<NURL> listener);

    /**
     * 取消订阅.
     * 
     * 取消订阅需处理契约：<br>
     * 1. 如果没有订阅，直接忽略。<br>
     * 2. 按全NURL匹配取消订阅。<br>
     * 
     * @param nurl 订阅条件，不允许为空，如：consumer://10.20.153.10/cn.ms.test.TestService?version=1.0.0&application=kylin
     * @param listener 变更事件监听器，不允许为空
     */
    void unsubscribe(NURL nurl, CoonListener<NURL> listener);

    /**
     * 查询符合条件的已注册数据，与订阅的推模式相对应，这里为拉模式，只返回一次结果。
     * 
     * @param nurl 查询条件，不允许为空，如：consumer://10.20.153.10/cn.ms.test.TestService?version=1.0.0&application=kylin
     * @return 已注册信息列表，可能为空。
     */
    List<NURL> lookup(NURL nurl);

}