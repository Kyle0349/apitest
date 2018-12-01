package com.kyle.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkUtils {


    public static JavaSparkContext getSparkContext(){

        SparkConf conf = new SparkConf().setMaster("local").setAppName("sparkTest01");
        JavaSparkContext sc = new JavaSparkContext(conf);

        return sc;


    }



}
