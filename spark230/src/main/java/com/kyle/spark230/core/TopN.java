package com.kyle.spark230.core;


import com.kyle.spark230.utils.SparkUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.compress.archivers.cpio.CpioArchiveEntry;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 class1 98
 class1 90
 class1 90
 class1 88
 class1 74
 class1 67
 class2 96
 class2 95
 class2 87
 class2 77
 class2 76
 class2 56
 class3 90
 class3 89
 class3 47
 class3 89
 class3 78
 class3 56
 */
public class TopN {

    public void topN(String filePath) throws InterruptedException {
        JavaSparkContext jsc = SparkUtils.getJsc();
        Broadcast<Integer> broadcast = jsc.broadcast(3);
        JavaRDD<String> linesRdd = jsc.textFile(filePath);
        JavaPairRDD<String, Integer> mapPair = linesRdd.mapToPair(line -> {
            String[] split = line.split(" ");
            return new Tuple2<>(split[0], Integer.valueOf(split[1]));
        });
        JavaPairRDD<String, Iterable<Integer>> groupRdd = mapPair.groupByKey();
        JavaRDD<Tuple2> mapRdd = groupRdd.map(gp -> {
            List list = IteratorUtils.toList(gp._2.iterator());
            Collections.sort(list);
            Collections.reverse(list);
            List list1 = list.subList(0, broadcast.getValue());
            return new Tuple2<>(gp._1, list1.iterator());
        });
        mapRdd.foreachPartition( fp -> {
            while (fp.hasNext()){
                Tuple2<String, Iterator<Integer>> next = fp.next();
                System.out.println(next._1);
                Iterator<Integer> it = next._2;
                while (it.hasNext()){
                    Integer next1 = it.next();
                    System.out.println(next1);
                }
                System.out.println("============");
            }
        });
        Thread.sleep(10000000);
    }


    public void testTopN2(String filePath){
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<String> linesRdd = jsc.textFile(filePath);
        JavaPairRDD<String, String> pairRdd = linesRdd.mapToPair(line -> {
            String[] split = line.split(" ");
            return new Tuple2<>(split[0], split[1]);
        });
        JavaPairRDD<String, Iterable<String>> groupRdd = pairRdd.groupByKey();
        JavaRDD<Tuple2<String, List>> mapRdd = groupRdd.map(gr -> {
            Iterable<String> strings = gr._2;
            List list = IteratorUtils.toList(strings.iterator());
            Collections.sort(list);
            Collections.reverse(list);
            return new Tuple2<>(gr._1, list);
        });
        mapRdd.foreachPartition( fp -> {
             while (fp.hasNext()){
                 Tuple2<String, List> next = fp.next();
                 System.out.println(next._1 + ": " + next._2);
             }
        });



    }










}
