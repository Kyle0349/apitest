package com.kyle.spark230.core;

import com.kyle.spark230.utils.MySort;
import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class GoodFunc implements Serializable {


    public void repartitionAndSortWithinPartitions01(){

        List<Tuple2<Integer,Integer>> list = Arrays.asList(
                new Tuple2<Integer,Integer>(2, 30),
                new Tuple2<Integer,Integer>(1, 2),
                new Tuple2<Integer,Integer>(6, 7),
                new Tuple2<Integer,Integer>(3, 4),
                new Tuple2<Integer,Integer>(5, 6),
                new Tuple2<Integer,Integer>(4, 5)
        );

        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaPairRDD<Integer, Integer> pairRDD = jsc.parallelizePairs(list);
        JavaPairRDD<Integer, Integer> sortRDD = pairRDD.repartitionAndSortWithinPartitions(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                return Integer.valueOf(key + "") % numPartitions();
            }
            @Override
            public int numPartitions() {
                return 3;
            }
        }, new MySort());

        System.out.println("partitions: " + sortRDD.getNumPartitions());

        sortRDD.foreachPartition( fp -> {
            while (fp.hasNext()){
                System.out.println(fp.next());
            }
            System.out.println("======");

        });

    }






}

























