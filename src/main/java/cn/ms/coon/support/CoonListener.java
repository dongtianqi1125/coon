package cn.ms.coon.support;

import java.util.List;

public interface CoonListener<T> {

	/**
	 * 当收到服务变更通知时触发。
	 * 
	 * @param list 已注册信息列表，总不为空
	 */
	void notify(List<T> list);

}