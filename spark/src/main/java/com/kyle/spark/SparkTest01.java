package com.kyle.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Arrays;

/**
 * map,
 * flatMap,
 * filter,
 * mapPartitions,
 * mapPartitionsWithIndex,
 */
public class SparkTest01 implements Serializable {


    /**
     *  mapFunc
     * @param sc
     */
    public void mapFunc(JavaSparkContext sc){

        JavaRDD<String> javaRDD01 = sc.parallelize(Arrays.asList("hello", "hi hadoop"));
        JavaRDD<String> javaRDD02 = javaRDD01.map(new Function<String, String>() {
            public String call(String line) throws Exception {
                return "line: " + line;
            }
        });

        javaRDD02.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


    }


    /**
     * flatMap
     * @param sc
     */
    public void flatMapFunc(JavaSparkContext sc){
        JavaRDD<String> javaRDD01 = sc.parallelize(Arrays.asList("hello", "hi hadoop"));
        JavaRDD<String> stringJavaRDD = javaRDD01.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                String[] val = s.split(" ");
                return Arrays.asList(val);
            }
        });

        stringJavaRDD.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }



}
