package com.kyle.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class HbaseConnection {

    String hbase_zookeeper_quorum;
    String hbase_client_ipc_pool_size;
    String hbase_zookeeper_property_clientPort;
    String zookeeper_znode_parent;
    String hbase_hconnection_threads_max;       //连接池最大连接数
    String hbase_hconnection_threads_core;      //连接池最小连接数
    String hbase_hconnection_threads_keepalivetime;     //空闲时间


    private static final Object lock=new Object();

    private static final Logger logger = LogManager.getLogger(HbaseConnection.class);

    private Connection connection = null;
    private Configuration conf = null;

    public HbaseConnection() {

    }

    private Configuration initConf() {
        conf = HBaseConfiguration.create();
        conf.set("zookeeper.znode.parent",zookeeper_znode_parent);
        conf.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum);
        conf.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_property_clientPort);
        //conf.set("hbase.client.ipc.pool.size", hbase_client_ipc_pool_size);
        conf.set("hbase.hconnection.threads.max", hbase_hconnection_threads_max);
        conf.set("hbase.hconnection.threads.core", hbase_hconnection_threads_core);
        conf.set("hbase.hconnection.threads.keepalivetime", hbase_hconnection_threads_keepalivetime);
        return conf;
    }

    public Configuration getConf(){
        if (null == conf){
            initConf();
        }
        return conf;
    }


    public Connection getConn(){
        synchronized (lock){
            if (connection == null){
                logger.debug("connection is null, begin to reconnect");
                reconnect();
            }
            return connection;
        }
    }

    private void reconnect(){
        try{
            if (null != connection){
                connection.close();
            }
            if (null == conf){
                initConf();
            }
            connection = ConnectionFactory.createConnection(conf);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

}
