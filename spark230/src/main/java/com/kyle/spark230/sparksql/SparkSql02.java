package com.kyle.spark230.sparksql;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql02 {



    public void createHiveTable(String sql){

    }


    public static void main(String[] args) {


        SparkSession hiveSparkSession = SparkUtils.getHiveSparkSession();
        hiveSparkSession.sql("use test");
        Dataset<Row> ds = hiveSparkSession.sql("select * from raw_user limit 50");

        JavaRDD<Row> rowJavaRDD = ds.toJavaRDD().repartition(2);
        System.out.println(rowJavaRDD.getNumPartitions());


        rowJavaRDD.foreachPartition( fp -> {
            int i = 0;
            while (fp.hasNext()){
                System.out.println(i++);
                Row next = fp.next();
                System.out.println(next.getString(0) + ":" + next.getString(1));
                Thread.sleep(1000);
            }
        });





//        Iterator<Row> rowIterator = ds.toLocalIterator();
//        while (rowIterator.hasNext()){
//            Row next = rowIterator.next();
//            System.out.println(next.getString(0));
//        }

    }


}
