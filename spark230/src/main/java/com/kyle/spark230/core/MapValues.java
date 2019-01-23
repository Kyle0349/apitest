package com.kyle.spark230.core;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class MapValues {

    public static void main(String[] args) {

        JavaSparkContext jsc = SparkUtils.getJsc();
        String[] ss = {"apple", "banana", "lemon"};
        String[] sss = {"grapes"};
        JavaRDD<Tuple2<String, String[]>> lineRdd = jsc.parallelize(Arrays.asList(new Tuple2("a", ss), new Tuple2("b", sss)));
        JavaPairRDD<String, String[]> pairRdd = lineRdd.mapToPair(rdd -> new Tuple2<>(rdd._1, rdd._2));
        System.out.println(pairRdd.getNumPartitions());

        JavaPairRDD<String, Integer> pairRDD = pairRdd.mapValues(v -> v.length);

        System.out.println(pairRDD.getNumPartitions());
        pairRDD.foreach( (a) -> System.out.println(a._1 + ": " + a._2));

    }

}