package com.kyle.spark230.sparksql;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonToHive {

    public void json2Hive01(){

        SparkSession hiveSparkSession = SparkUtils.getHiveSparkSession();
        Dataset<Row> json = hiveSparkSession.read().json("file:///tmp/local_users.json");


        json.select("info.monry").show();

        json.printSchema();






    }



}
