package com.kyle.spark;

import com.kyle.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class SparkTest03 {


    public static void main(String[] args) {

        JavaSparkContext sc = SparkUtils.getSparkContext();

        JavaRDD<String> javaRDD01 = sc.parallelize(Arrays.asList("hello", "hi hadoop"));


        JavaPairRDD<String, Integer> javaRDD02 = javaRDD01.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String line) throws Exception {
                return null;
            }
        });


        javaRDD02.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + ": " + stringIntegerTuple2._2);
            }
        });

        sc.close();



    }


}
