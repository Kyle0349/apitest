package com.kyle.spark230.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;


/**
 * CreateMode.EPHEMERAL; 持久节点
 * CreateMode.EPHEMERAL_SEQUENTIAL; 持久顺序节点
 * CreateMode.PERSISTENT; 临时节点
 * CreateMode.PERSISTENT_SEQUENTIAL; 临时顺序节点
 *
 * OPEN_ACL_UNSAFE：完全开放
 * CREATOR_ALL_ACL：创建该znode的连接拥有所有权限
 * READ_ACL_UNSAFE：所有的客户端都可读
 *
 */
public class ZKUtils implements Serializable {

    private static ZooKeeper zkClient = null;
    private static String parentPath = "/";

    static {
        try {
            zkClient = new ZooKeeper("centos1:2181", 5000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("path: " + event.getPath());
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *

     *
     * @param zkPath
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void createNode(String zkPath) throws KeeperException, InterruptedException {
        if (zkClient.exists(zkPath, false) == null){
            zkClient.create(zkPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }


    /**
     *
     * @param zkPath
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void createDataNode(String zkPath, String data) throws KeeperException, InterruptedException {
        if (StringUtils.isBlank(zkPath)){
            throw new RuntimeException("zkPath can not be null");
        }
        if (zkClient.exists(zkPath, false) == null){
            zkClient.create(zkPath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }else {
            throw new RuntimeException("zkPath is already exist");
        }
    }


    /**
     *
     * @param zkPath
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void createOrUpdateDataNode(String zkPath, String data) throws KeeperException, InterruptedException {
        if (StringUtils.isBlank(zkPath)){
            throw new RuntimeException("zkPath can not be null");
        }
        if (zkClient.exists(zkPath, false) == null){
            zkClient.create(zkPath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }else {
            zkClient.setData(zkPath, data.getBytes(), -1);
        }
    }


    /**
     *
     * @param zkPath
     * @param data
     * @param version
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void updateDataNode(String zkPath, String data, int version) throws KeeperException, InterruptedException {
        if (StringUtils.isBlank(zkPath)){
            throw new RuntimeException("zkPath can not be null");
        }
        if (!isExist(zkPath)){
            throw new RuntimeException("zkPath does not exist");
        }
        zkClient.setData(zkPath, data.getBytes(), version);
    }



    /**
     *
     * @param zkPath
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static String getZKValue(String zkPath) throws KeeperException, InterruptedException {
        String data = null;
        if (StringUtils.isBlank(zkPath)){
            throw new RuntimeException("zkPath can not be null");
        }
        if (zkClient.exists(zkPath, false) != null){
            data = new String(zkClient.getData(zkPath, false, null));
        }
        return data;
    }


    /**
     *
     * @param zkPath
     * @return 存在：true 不存在 false
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static boolean isExist(String zkPath) throws KeeperException, InterruptedException {
        return zkClient.exists(zkPath, false) != null;
    }


    /**
     *
     * @param zkPath
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static List<String> getChildren(String zkPath) throws KeeperException, InterruptedException {
        return  zkClient.getChildren(zkPath, false);
    }






}

