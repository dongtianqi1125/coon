package cn.ms.coon;

import java.util.List;

import cn.ms.coon.support.common.Mcf;
import cn.ms.neural.NURL;

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
public interface Mconf {

	/**
	 * Connect configuration center
	 */
	void connect(NURL nurl);

	/**
	 * Configuration center status
	 * 
	 * @return
	 */
	boolean available();

	/**
	 * The Add Configuration Data.
	 * 
	 * @param mcf
	 * @param obj
	 */
	void addConf(Mcf mcf, Object obj);

	/**
	 * The Delete Configuration Data.<br>
	 * <br>
	 * Prompt：<br>
	 * 1.Set parameter 'data'：Delete a data.<br>
	 * 2.Not set parameter 'data'：Delete a conf.<br>
	 * <br>
	 * 
	 * @param mcf
	 */
	void delConf(Mcf mcf);

	/**
	 * The Update Configuration Data.
	 * 
	 * @param mcf
	 * @param obj
	 */
	void upConf(Mcf mcf, Object obj);

	/**
	 * The Pull Configuration Data.
	 * 
	 * @param mcf
	 * @param cls
	 * @return
	 */
	<T> T pull(Mcf mcf, Class<T> cls);

	/**
	 * The Pulls Configuration Data.
	 * 
	 * @param mcf
	 * @param cls
	 * @return
	 */
	<T> List<T> pulls(Mcf mcf, Class<T> cls);

	/**
	 * The Push Configuration Data.
	 * 
	 * @param mcf
	 * @param cls
	 * @param listener
	 * @return
	 */
	<T> void push(Mcf mcf, Class<T> cls, ServiceListener<T> listener);

	/**
	 * The UnPush Configuration Data.
	 * 
	 * @param mcf
	 */
	void unpush(Mcf mcf);

	void destroy();

}
