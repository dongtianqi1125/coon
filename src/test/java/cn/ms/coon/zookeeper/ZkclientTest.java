package cn.ms.coon.zookeeper;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

public class ZkclientTest {
	static final String CONNECT_ADDR = "127.0.0.1:2181";  
    static final int SESSION_TIMEOUT = 50000;  
  
    public static void main(String[] args) throws InterruptedException {  
        ZkClient zkClient = new ZkClient(new ZkConnection(CONNECT_ADDR, SESSION_TIMEOUT));  
        zkClient.subscribeDataChanges("/super", new IZkDataListener() {  
            @Override  
            public void handleDataChange(String s, Object o) throws Exception {  
                System.out.println("变更节点为：" + s + "，变更数据为：" + o);  
            }  
  
            @Override  
            public void handleDataDeleted(String s) throws Exception {  
                System.out.println("删除的节点为：" + s);  
            }  
        });  
        zkClient.createPersistent("/super", "123");  
        Thread.sleep(3000);  
        zkClient.writeData("/super", "456", -1);  
        Thread.sleep(1000);  
        zkClient.createPersistent("/super/c1", "789"); //不会被监控到  
        zkClient.deleteRecursive("/super");  
        Thread.sleep(Integer.MAX_VALUE);  
    }  
}
