package com.kyle.spark230.sparkstreaming;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

public class NomalFunc implements Serializable {

    public void reduce01(String ip, int port) throws InterruptedException {

        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        JavaReceiverInputDStream<String> socketTextStream = jssc.socketTextStream(ip, port);
        JavaDStream<String> reduce = socketTextStream.reduce(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + "," + v2;
            }
        });
        reduce.foreachRDD( rdd -> {
            if (!rdd.isEmpty()){
                rdd.foreach( x -> System.out.println(x));
            }

        });
        jssc.start();
        jssc.awaitTermination();

    }




}
