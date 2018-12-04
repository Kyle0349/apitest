package com.kyle.spark160.utils;

import org.apache.hadoop.log.LogLevel;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkUtils {


    public static JavaSparkContext getSparkContext(){

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("sparkTest01")
                .set("spark.default.parallelism", "1");
        JavaSparkContext sc = new JavaSparkContext(conf);

        return sc;


    }


    public static JavaStreamingContext getStreamingContext(){
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("StreamTest")
                .set("spark.default.parallelism", "2");

        JavaSparkContext jsc = getSparkContext();
        jsc.setLogLevel("WARN");

        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(10));
        return jssc;

    }



}
