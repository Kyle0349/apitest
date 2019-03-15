package com.kyle.spark230.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class SparkUtils {

    public static SparkConf getSparkcONF(){
        return  new SparkConf()
                .setAppName("spatk230Test")
                //.set("spark.default.parallelism", "2")
                //.set("spark.reducer.maxSizeInFlight", "24")
                .set("spark.executor.memory", "512m")
                .set("spark.streaming.kafka.maxRatePerPartition", "30") //控制spark获取kafka每个分区每秒最大数据量
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator","com.kyle.spark230.utils.MyRegistrator") //解决序列化
                .setMaster("local[3]");
    }

    public static JavaStreamingContext getStreamingContext(SparkConf conf){
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");
        return new JavaStreamingContext(jsc, Durations.seconds(30));
    }


    public static JavaSparkContext getJscRunOnYarn(){
        SparkConf conf = new SparkConf()
                .setAppName("spark_apt_test");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        return jsc;

    }


    public static JavaSparkContext getJsc(){
        SparkConf conf = new SparkConf()
                .setAppName("spatk230Test")
                //.set("spark.locality.wait","10")
                //.set("spark.default.parallelism", "2")
                //.set("spark.reducer.maxSizeInFlight", "24")
                .set("spark.executor.memory", "1024m")
                .set("spark.driver.memory","1g")
                //.set("spark.shuffle.file.buffer","64")
                //.set("spark.shuffle.memoryFraction","0.4")
                .setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");
        return jsc;
    }


    public static JavaSparkContext getJscOnYarn(){
        System.setProperty("HADOOP_USER_NAME", "yarn");
        String[] jars = {"/Users/kyle/Documents/kyle/sourceCode/apitest/out/artifacts/spark_test/spark-test.jar"};
        SparkConf conf = new SparkConf()
                .set("yarn.resourcemanager.hostname", "172.16.131.101")
                .set("spark.driver.memory", "512m")
                .set("spark.executor.memory", "512m")
                .set("spark.executor.instances", "1")
                .set("spark.default.parallelism", "1")
                .set("spark.driver.host", "172.16.131.1")
                .set("yarn.application.classpath", "/opt/cloudera/parcels/SPARK2/lib/spark2/jars/*:/opt/cloudera/parcels/CDH/jars/*")
                //.set("spark.yarn.preserve.staging.files", "false")
                //.set("spark.yarn.dist.files", "yarn-site.xml")
                .setJars(jars)
                .setAppName("spatk230Test")
                .setMaster("yarn-client");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");
        return jsc;
    }

    public static JavaStreamingContext getStreamingContext(){
        SparkConf conf = new SparkConf()
                .setMaster("local[3]")
                .setAppName("Stream230Test")
                .set("spark.executor.memory", "512m")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator","com.kyle.spark230.utils.MyRegistrator")
                .set("spark.default.parallelism", "1");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");

        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(2));
        return jssc;

    }

    public static SparkSession getSparkSession(){
        SparkSession session = SparkSession.builder()
                .master("local[3]")
                .appName("SparkSession01")
                .config("spark.executor.memory", "450m")
                .config("spark.driver.memory", "1073741824")
                .getOrCreate();
        return session;
    }


    public static SparkSession getHiveSparkSession(){
        SparkSession session = SparkSession.builder()
                .appName("spark_hive_opera")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions","100")
                .config("warehouselocation", "hdfs://cdh01:8020/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();
        return session;
    }





}
