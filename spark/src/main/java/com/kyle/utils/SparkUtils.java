package com.kyle.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class SparkUtils {


    public static JavaSparkContext getSparkContext(){

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("sparkTest01")
                .set("spark.default.parallelism", "1");
        JavaSparkContext sc = new JavaSparkContext(conf);

        return sc;


    }



}
