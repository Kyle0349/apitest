package com.kyle.spark230.main;

import com.kyle.spark230.core.GetMetaInfoFromRMDB;
import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.sql.SparkSession;

public class SyncTableFromMysql {

    public static void main(String[] args) {

        GetMetaInfoFromRMDB getMetaInfoFromRMDB = new GetMetaInfoFromRMDB();
        String metaInfoFromMysql = getMetaInfoFromRMDB.getMetaInfoFromMysql("test","product");
        System.out.println(metaInfoFromMysql);

        //String createStmt = metaInfoFromMysql + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' STORED AS PARQUET";
        String createStmt = metaInfoFromMysql + " STORED AS PARQUET";

        System.out.println(createStmt);

        SparkSession hiveSparkSession = SparkUtils.getHiveSparkSession();

        hiveSparkSession.sql("use test");
        hiveSparkSession.sql(createStmt);


    }


}
