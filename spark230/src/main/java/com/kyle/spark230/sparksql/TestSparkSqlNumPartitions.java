package com.kyle.spark230.sparksql;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class TestSparkSqlNumPartitions {



    public void createHiveTable(String sql){

    }


    public  void testNumPartitions(String db, String sql) {
        SparkSession hiveSparkSession = SparkUtils.getHiveSparkSession();
        hiveSparkSession.sql("use " + db);

        Dataset<Row> ds = hiveSparkSession.sql(sql);

        System.out.println(ds.toJavaRDD().getNumPartitions()); //8











//        Dataset<Row> repartition = ds.repartition(3);
//        JavaRDD<Row> rowJavaRDD = repartition.toJavaRDD();
//        System.out.println(rowJavaRDD.getNumPartitions());
//        rowJavaRDD.foreachPartition( fp -> {
//            int i = 0;
//
//            while (fp.hasNext()){
//                System.out.println(i++);
//                Row next = fp.next();
//                System.out.println(next.getString(0) + ":" + next.getString(1));
//                System.out.println("=========================");
//                Thread.sleep(1000);
//            }
//        });

    }



    public void testTasksReadHiveTable(String url, String table, String[] predicates, Properties prop) throws ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        SparkSession hiveSparkSession = SparkUtils.getHiveSparkSession();
        Dataset<Row> jdbc = hiveSparkSession.read().jdbc(url, table, predicates, prop);

        jdbc.show();

    }



    public void testTasksReadHiveTable1(String url, String table,Properties prop) throws ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        SparkSession hiveSparkSession = SparkUtils.getHiveSparkSession();
        Dataset<Row> jdbc = hiveSparkSession.read().jdbc(url, table, prop);
        jdbc.show();
    }



}
