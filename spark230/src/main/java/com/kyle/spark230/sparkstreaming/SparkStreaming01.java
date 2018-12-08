package com.kyle.spark230.sparkstreaming;

import com.kyle.spark230.utils.MyKafkaUtils;
import com.kyle.spark230.utils.SparkUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.zookeeper.KeeperException;

import java.io.Serializable;
import java.util.*;

public class SparkStreaming01 implements Serializable {

    private static  Broadcast<Map<String, Object>> kafkaParamsBroadcast = null;


    public void readFromKafka() throws InterruptedException, KeeperException {

        JavaStreamingContext jssc = SparkUtils.getStreamingContext();

        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "centos1:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "com.kyle.test");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("topic02");
        kafkaParamsBroadcast = jssc.sparkContext().broadcast(kafkaParams);

        Map<TopicPartition, Long> offsets = MyKafkaUtils.getOffset("topic02", Arrays.asList(0, 1, 2));
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams, offsets)
        );


        directStream.foreachRDD(
                new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
                    @Override
                    public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD) throws Exception {

                        consumerRecordJavaRDD.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
                            @Override
                            public void call(Iterator<ConsumerRecord<String, String>> consumerRecordIterator) throws Exception {
                                while (consumerRecordIterator.hasNext()){
                                    ConsumerRecord<String, String> record = consumerRecordIterator.next();
                                    String topic = record.topic();
                                    int partition = record.partition();
                                    String key = record.key();
                                    String value = record.value();
                                    long offset = record.offset();
                                    System.out.println(topic + "-" + partition + "-" + offset + ": " + key + ": " + value);
                                }
                            }
                        });



                        //业务逻辑代码
//                        consumerRecordJavaRDD.mapToPair(new PairFunction<ConsumerRecord<String,String>, String, String>() {
//                            @Override
//                            public Tuple2<String, String> call(ConsumerRecord<String, String> record) throws Exception {
//                                return new Tuple2<>(record.key(), record.value());
//                            }
//                        }).foreach(new VoidFunction<Tuple2<String, String>>() {
//                            @Override
//                            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
//                                System.out.println(stringStringTuple2._1 + ": " + stringStringTuple2._2);
//                            }
//                        });





                        //更新offset到zookeeper
//                        OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();
//                        OffsetRange offset = offsetRanges[TaskContext.get().partitionId()];
//                        System.out.println(offset.topic() + ": " + offset.partition()
//                                + ": " + offset.fromOffset() + ": " + offset.untilOffset());
//                        TopicPartition topicPartition = new TopicPartition(offset.topic(), offset.partition());
//                        MyKafkaUtils.updateOffset(
//                                String.valueOf(kafkaParamsBroadcast.getValue().get("group.id")),
//                                topicPartition, offset.untilOffset());

                    }
                }
        );
        jssc.start();
        jssc.awaitTermination();
    }
}
