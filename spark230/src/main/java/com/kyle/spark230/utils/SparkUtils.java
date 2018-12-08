package com.kyle.spark230.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkUtils {


    public static JavaSparkContext getJsc(){
        SparkConf conf = new SparkConf()
                .setAppName("spatk230Test")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        return jsc;
    }

    public static JavaStreamingContext getStreamingContext(){
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Stream230Test")
                .set("spark.default.parallelism", "2");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");

        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(3));
        return jssc;

    }


    public static SparkSession getSparkSession(){
        SparkSession session = SparkSession.builder()
                .master("local")
                .appName("SparkSession01")
                .config("spark.driver.memory", "2147480000")
                .getOrCreate();
        return session;
    }


}
