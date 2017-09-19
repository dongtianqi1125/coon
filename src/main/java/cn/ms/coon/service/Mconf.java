package cn.ms.coon.service;

import io.neural.extension.NSPI;

import java.util.List;
import java.util.Map;

import cn.ms.coon.Coon;
import cn.ms.coon.support.CoonListener;
import cn.ms.coon.support.mconf.Mcf;

/**
 * The MicroService Configuration Center.<br>
 * <br>
 * Configuration center data structure:<br>
 * ①--> /mconf?……<br>
 * ②--> /[app]?node=[node]&……<br>
 * ③--> /[conf]?env=[env]&group=[group]&version=[version]&……<br>
 * ④--> /[data]?……<br>
 * ⑤--> {JSON Data String}<br>
 * <br>
 * <br>
 * Connect URL:<br>
 * [zookeeper/redis]://127.0.0.1:2181/mconf?node=[node]&app=[app]&env=[env]&conf
 * =[conf]&category=[category]&version=[version]&data=[data]&……<br>
 * <br>
 * <br>
 * The data structure：<br>
 * prefixKey(①+②+③)--> /mconf?……/[app]?node=[node]&……/[conf]?env=[env]&……<br>
 * suffixKey(④)--> /[data]?group=[group]&version=[version]&……<br>
 * Data String(⑤)--> JSON String<br>
 * <br>
 * Zookeeper< Path, Data> ——> <①+②+③+④, ⑤> ——> Push<br>
 * Redis< Key, Value> ——> <①+②+③, Map<④, ⑤>> ——> Pull<br>
 * 
 * @author lry
 */
@NSPI("zookeeper")
public interface Mconf extends Coon {

	void publish(Mcf mcf, Object obj);

	void unpublish(Mcf mcf, Object obj);

	<T> void subscribe(Mcf mcf, Class<T> cls, CoonListener<T> listener);

	<T> void unsubscribe(Mcf mcf, CoonListener<T> listener);

	<T> T lookup(Mcf mcf, Class<T> cls);
	
	<T> List<T> lookups(Mcf mcf, Class<T> cls);
	
	Map<String, Map<String, String>> apps();
	
	Map<String, Map<String, String>> confs();

}
