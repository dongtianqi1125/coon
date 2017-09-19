package cn.ms.coon.service;

import io.neural.extension.NSPI;
import cn.ms.coon.Coon;

/**
 * The MicroService Distributed Lock Center.
 * 
 * @author lry
 */
@NSPI("zookeeper")
public interface Mlock extends Coon {

}
