package com.kyle.sparkStream;


import com.kyle.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;

/**
 * stream 读取数据
 */
public class StreamTest01 implements Serializable {


    public void readFromSocket(){

        JavaStreamingContext jssc = SparkUtils.getStreamingContext();

        final JavaReceiverInputDStream<String> lineDstream = jssc.socketTextStream("localhost",9999);


        lineDstream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            public void call(JavaRDD<String> lineRdd) throws Exception {

                lineRdd.foreach(new VoidFunction<String>() {
                    public void call(String line) throws Exception {
                        System.out.println("line: " + line);
                    }
                });
            }
        });


        jssc.start();
        jssc.awaitTermination();


    }



    public void readFromKafka(){




    }




}
