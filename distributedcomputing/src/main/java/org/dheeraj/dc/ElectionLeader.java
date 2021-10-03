package org.dheeraj.dc;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Hello world!
 *
 */
public class ElectionLeader implements Watcher
{

    private static final String ZOOKEEPER_URL = "localhost:2181";
    private static final int TIMEOUT = 500;
    private static final String NAMESPACE = "/election";
    private ZooKeeper zooKeeper;
    private String nodeName = "";

    public static void main( String[] args )
    {
        ElectionLeader leader = new ElectionLeader();
        try {
            leader.connectToZookeeperServer();
            leader.volunteerForLeaderShip();
            leader.electLeader();
            leader.run();
            leader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        };
    }

    private void electLeader() throws KeeperException, InterruptedException {
        List<String> allChildrens = this.zooKeeper.getChildren(NAMESPACE, false);
        Collections.sort(allChildrens);

        if(allChildrens.get(0).equals(this.nodeName)){
            System.out.println("i am the leader");
        }else{
            System.out.println(allChildrens.get(0) +" is the leader");
            String leaderNode = allChildrens.get(0);
            Stat stat = this.zooKeeper.exists(NAMESPACE+"/"+leaderNode, this);
            if(stat != null){
                System.out.println("watching node "+leaderNode);
            }
        }
    }

    public void connectToZookeeperServer() throws IOException{
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_URL, TIMEOUT, this);
        System.out.println(zooKeeper);
    }


    public void run() throws InterruptedException{
        synchronized(zooKeeper){
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException{
        zooKeeper.close();
    }


    public void volunteerForLeaderShip() throws KeeperException, InterruptedException{
        String zNodePrefix = NAMESPACE+"/_c";
        String zNodePath = this.zooKeeper.create(zNodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("z Node created with path "+zNodePath);
        this.nodeName = zNodePath.substring(zNodePath.lastIndexOf("/")+1);
        System.out.print("z Node created with "+this.nodeName+", and ");
    }

    public void getLeaderStatus() throws IllegalStateException, InterruptedException, KeeperException{
       List<String> childrenList =  this.zooKeeper.getChildren(NAMESPACE, false);
       System.out.println(childrenList);
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if(event.getState().equals(Event.KeeperState.SyncConnected)){
                    System.out.println("Connected to zoo keeper servers..");
                }else if(event.getState().equals(Event.KeeperState.Disconnected)){
                    System.out.println("Oops..Disconnected from zoo keeper servers..");
                    synchronized(zooKeeper){
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                System.out.println("watched node deleted "+event.getState());
                try {
                    electLeader();
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            default:
                break;
        }
        
    }
}
