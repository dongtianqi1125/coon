package cn.ms.coon.service;

import cn.ms.coon.Coon;
import cn.ms.neural.extension.NSPI;

/**
 * The MicroService Distributed Lock Center.
 * 
 * @author lry
 */
@NSPI("zookeeper")
public interface Mlock extends Coon {

}
