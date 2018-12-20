package com.kyle.spark230.main;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) {

        JavaSparkContext jsc = SparkUtils.getJscRunOnYarn();
        JavaRDD<String> linesRdd = jsc.textFile("hdfs://centos1:8020/tmp/test/testData");

        JavaPairRDD<String, Integer> resultRdd = linesRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((v1, v2) -> v1 + v2);

        for (Tuple2<String, Integer> tuple2 : resultRdd.collect()) {
            System.out.println(tuple2._1 + ": " + tuple2._2);
        }

    }


}
