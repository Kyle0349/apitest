package com.kyle.spark230.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;

public class WindowFunc implements Serializable
{


    public void reduceByKeyAndWindow() throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Stream230Test");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(3));
        JavaReceiverInputDStream<String> inputDStream = jssc.socketTextStream("localhost", 9999);


        JavaDStream<String> words = inputDStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairDStream<String, Integer> rest =
                pairs.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(30), Durations.seconds(15));

        rest.foreachRDD( rdd -> rdd.foreach( res -> System.out.println(res._1 + ": " + res._2)));

        jssc.start();
        jssc.awaitTermination();

    }


    /**
     * 优化
     * @throws InterruptedException
     */
    public void reduceByKeyAndWindow1() throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Stream230Test");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(3));

        jssc.checkpoint("/Users/kyle/Desktop/sss");

        JavaReceiverInputDStream<String> inputDStream = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = inputDStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairDStream<String, Integer> rest = pairs.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 - v2;
            }
        }, Durations.seconds(15), Durations.seconds(6));

        rest.foreachRDD( rdd -> rdd.foreach( res -> System.out.println(res._1 + ": " + res._2)));
        jssc.start();
        jssc.awaitTermination();

    }


}
