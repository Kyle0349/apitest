package com.kyle.spark230.core;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class TestTaskReadFile {

    public void tasksReadFile(String filePath) throws InterruptedException {

        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<String> linesRdd = jsc.textFile(filePath, 12);
        System.out.println(linesRdd.getNumPartitions());



        JavaPairRDD<String, String> pairRdd = linesRdd.mapToPair(line -> {
            String[] split = line.split(" ");
            return new Tuple2<>(split[0], line);
        });
        System.out.println(pairRdd.getNumPartitions());


        JavaPairRDD<String, Iterable<String>> groupRdd = pairRdd.groupByKey();
        System.out.println(groupRdd.getNumPartitions());





//        linesRdd.foreachPartition( fp -> {
//            int i = 0;
//            while (fp.hasNext()){
//                System.out.println(i++);
//                String next = fp.next();
//                System.out.println(next);
//                System.out.println("==========");
//                Thread.sleep(1000);
//            }
//        });
//        Thread.sleep(10000000);


    }


}
