package com.kyle.spark230.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Properties;

public class GetMetaInfoFromRMDB {

    public String getMetaInfoFromMysql(String db, String table){

        SparkSession session = SparkSession.builder().appName("SyncTableFromMysql").getOrCreate();
        //SparkSession session = SparkUtils.getSparkSession();
        Properties prop = new Properties();

        String sql = "(select CONCAT('create table ', concat('sqoop_test_',t4.TABLE_NAME), '(', t4.column_info, ')', ' comment ', '\"', t4.TABLE_COMMENT, '\"') as create_stmt\n" +
                "from \n" +
                "(select \n" +
                "t3.TABLE_NAME, t3.TABLE_COMMENT, group_concat(CONCAT(t3.COLUMN_NAME, ' ', t3.DATA_TYPE, ' comment ', '\"', t3.COLUMN_COMMENT, '\"')) AS column_info \n" +
                "from \n" +
                "(select \n" +
                "t1.TABLE_NAME, \n" +
                "CASE WHEN t2.TABLE_COMMENT = NULL THEN t2.TABLE_COMMENT ELSE t2.TABLE_COMMENT END AS TABLE_COMMENT, \n" +
                "t1.COLUMN_NAME, \n" +
                "CASE WHEN t1.DATA_TYPE = 'varchar' THEN 'string' WHEN t1.DATA_TYPE = 'int' THEN 'int' WHEN t1.DATA_TYPE = 'datetime' THEN 'string' END AS DATA_TYPE, \n" +
                "CASE WHEN t1.COLUMN_COMMENT = NULL THEN COLUMN_NAME ELSE COLUMN_COMMENT END AS COLUMN_COMMENT\n" +
                "from COLUMNS t1 JOIN TABLES t2 ON t1.TABLE_NAME = t2.TABLE_NAME\n" +
                "where t1.TABLE_NAME = \"" + table + "\" and t1.TABLE_SCHEMA = \"" + db + "\" and t2.TABLE_SCHEMA = \"" + db + "\") t3 \n" +
                "GROUP BY t3.TABLE_NAME, t3.TABLE_COMMENT) t4) t5";

        String url = "jdbc:mysql://centos1:3306/information_schema";
        prop.setProperty("user", "root");
        prop.setProperty("password", "root");
        prop.setProperty("user", "root");
        prop.setProperty("driver", "com.mysql.jdbc.Driver");
        Dataset<Row> metaDs = session.read().jdbc(url, sql, prop);
        List<Row> collect = metaDs.javaRDD().collect();
        String rtn = null;
        for (Row row : collect) {
            if (row.length()>0){
                rtn = row.getString(0);
            }
        }
        session.close();
        return rtn;
    }

}
