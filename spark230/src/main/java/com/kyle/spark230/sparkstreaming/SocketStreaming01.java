package com.kyle.spark230.sparkstreaming;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SocketStreaming01 {

    public void socketStreaming01(String ip, int port) throws InterruptedException {
        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        JavaReceiverInputDStream<String> socketTextStream = jssc.socketTextStream(ip, port);

        socketTextStream.foreachRDD( rdd -> {
            rdd.foreach( s -> System.out.println(s));
        });

        jssc.start();
        jssc.awaitTermination();


    }


}
