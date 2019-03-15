package com.kyle.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HbaseUtils {


    private static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "cdh01:2181,cdh02:2181,cdh03:2181");
    }


    public static Connection getConn() throws IOException {
        return ConnectionFactory.createConnection(conf);
    }

    public static Configuration getConf(){
        return conf;
    }


    public static Admin getHBaseAdmin() throws IOException {
        return ConnectionFactory.createConnection(conf).getAdmin();
    }

    public static void close(Admin admin, Connection conn) throws IOException {
        close(admin);
        close(conn);
    }

    public static void close(Table table) throws IOException {
        if (table != null){
            table.close();
        }
    }

    public static void close(Admin admin) throws IOException {
        if (admin != null){
            admin.close();
        }
    }

    public static void close(Connection conn) throws IOException {
        if (conn != null){
            conn.close();
        }
    }

    public static void close(Admin admin, Connection conn, Table table) throws IOException {
        close(table);
        close(admin);
        close(conn);
    }


}
