package com.kyle.sparkStream;


import com.kyle.utils.SparkUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * stream 读取数据
 */
public class StreamTest01 implements Serializable {


    /**
     * spark 从 socket 端口读取数据
     * @param host
     * @param port
     */
    public void readFromSocket(String host, int port){

        JavaStreamingContext jssc = SparkUtils.getStreamingContext();

        final JavaReceiverInputDStream<String> lineDstream = jssc.socketTextStream(host,port);


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


    /**
     * spark 从kafka读取数据
     */
    public void readFromKafka(){
        JavaStreamingContext jssc = SparkUtils.getStreamingContext();

        Map<String, String> kafkaParam = new HashMap<String, String>();
        kafkaParam.put("metadata.broker.list","cdh01:9092");
        Set<String> topicSet = new HashSet<String>();
        topicSet.add("topic01");
        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParam, topicSet);


        JavaDStream<String> valueDstream = directStream.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._2;
            }
        });

        valueDstream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.foreach(new VoidFunction<String>() {
                    public void call(String s) throws Exception {
                        System.out.println("msg: " + s);
                    }
                });
            }
        });

        jssc.start();
        jssc.awaitTermination();

    }


    /**
     * spark 从kafka读取数据，并将offset保存到zk上
     */
    public void readFromKafkaWithZK(){
        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        HashMap<String, String> kafkaParam = new HashMap<String, String>();
        kafkaParam.put("metadata.broker.list","cdh01:9092");
        Set<String> topicSet = new HashSet<String>();
        topicSet.add("topic01");

        KafkaCluster kafkaCluster = getKafkaCluster(kafkaParam);


    }


    /**
     * 将kafkaParams转换成scala map，用于创建kafkaCluster
     *
     * @param kafkaParams kafka参数配置
     * @return kafkaCluster管理工具类
     */
    private static KafkaCluster getKafkaCluster(Map<String, String> kafkaParams) {
        // 类型转换
        scala.collection.mutable.Map<String, String> tmpMap = JavaConversions.mapAsScalaMap(kafkaParams);

//        scala.collection.immutable.Map<String, String> immutableKafkaParam =
//                tmpMap.toMap(new Predef.$less$colon$less<>() {
//            private static final long serialVersionUID = 1L;
//            public Tuple2<String, String> apply(Tuple2<String, String> v1) {
//                return v1;
//            }
//        });

        return null;
    }


}
