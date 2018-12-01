package com.kyle.spark;

import com.kyle.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

public class SparkTest02 {

    public static void main(String[] args) {

        JavaSparkContext sc = SparkUtils.getSparkContext();

        JavaRDD<String> javaRDD01 = sc.parallelize(Arrays.asList("hello", "hi hadoop"));

        JavaRDD<String> javaRDD02 = javaRDD01.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String line) throws Exception {
                String[] split = line.split(" ");
                return Arrays.asList(split);
            }
        });

        javaRDD02.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();

    }


}
