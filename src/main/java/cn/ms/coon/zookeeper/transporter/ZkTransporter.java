package cn.ms.coon.zookeeper.transporter;

import java.util.List;
import java.util.Map;

import cn.ms.neural.NURL;
import cn.ms.neural.extension.NSPI;

@NSPI("curator")
public interface ZkTransporter {

	void connect(NURL nurl);
	boolean isConnected();
	NURL getNurl();
	void close();
	
	void create(String path, boolean ephemeral);
	void delete(String path);
	List<String> getChildren(String path);
	
	List<String> addChildListener(String path, ChildListener listener);
	void removeChildListener(String path, ChildListener listener);

	void addStateListener(StateListener listener);
	void removeStateListener(StateListener listener);

	void createData(String path, String json);

	public interface ChildListener {
		void childChanged(String path, List<String> children);
	}

	public interface StateListener {
		int DISCONNECTED = 0;
		int CONNECTED = 1;
		int RECONNECTED = 2;

		void stateChanged(int connected);
	}
	
	public interface DataListener {
		void dataChanged(String path, Map<String, String> childrenDatas);
	}

}
