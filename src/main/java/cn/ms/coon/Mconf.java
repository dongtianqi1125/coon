package cn.ms.coon;

import java.util.List;

import cn.ms.neural.NURL;
import cn.ms.neural.extension.NSPI;

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

	void connect(NURL nurl);

	void publish(NURL nurl, Object obj);

	void unpublish(NURL nurl, Object obj);

	<T> void subscribe(NURL nurl, CoonListener<T> listener);

	<T> void unsubscribe(NURL nurl, CoonListener<T> listener);

	<T> List<T> lookup(NURL nurl);

}
