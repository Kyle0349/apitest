package com.kyle.spark230.sparksql;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

public class SparkSql01 {

    public void readFromMysql1(SparkSession sparkSession, String url, String table, Properties properties){
        Dataset<Row> ds = sparkSession.read().format("jdbc").jdbc(url, table, properties);
        ds.show(10);

    }


    public void readFromMysql2(
            SparkSession sparkSession,
            String url,
            String table,
            String column,
            String lowerbound,
            String upperbound,
            int numPartitions) throws InterruptedException {


        Dataset<Row> ds = sparkSession.read().format("jdbc")
                .option("url", url)
                .option("dbtable", table)
                .option("user", "root")
                .option("password", "root")
                .option("numPartitions", numPartitions)
                .option("partitionColumn", column)
                .option("lowerBound", lowerbound)
                .option("upperBound", upperbound).load();

        ds.persist();


        Dataset<Row> item_id_count_ds = ds.groupBy("user_id")
                .agg(count("item_id").alias("item_id_count"));

        item_id_count_ds.show(10);

        item_id_count_ds.agg(sum("item_id_count")).show();

        Thread.sleep(1000*3000);

    }


    public void readFromMysql3(SparkSession sparkSession,
                               String url,
                               String table){

        Dataset<Row> ds = sparkSession.read().format("jdbc")
                .option("url", url)
                .option("dbtable", table)
                .option("user", "root")
                .option("password", "root")
                .load();


        ds.groupBy("user_id").agg(ImmutableMap.of("item_id", "count"))
                .show(10);
    }







}
