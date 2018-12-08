package com.kyle.spark230.sparkstreaming;

import com.kyle.spark230.utils.MyKafkaUtils;
import com.kyle.spark230.utils.SparkUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.apache.zookeeper.KeeperException;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SparkStreaming01 implements Serializable {

    private static  Broadcast<Map<String, Object>> kafkaParamsBroadcast = null;


    public void readFromKafka(Map<String, Object> kafkaParams, String topic, Integer[] partitions)
            throws InterruptedException, KeeperException {
        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        kafkaParamsBroadcast = jssc.sparkContext().broadcast(kafkaParams);
        Collection<String> topics = Arrays.asList(topic);
        Map<TopicPartition, Long> offsets = MyKafkaUtils.getOffsets(
                kafkaParams.get("group.id").toString(), topic, partitions);
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams, offsets)
        );


        //handle dataRdd
        directStream.foreachRDD(consumerRecordJavaRDD -> {
            if (!consumerRecordJavaRDD.isEmpty()){
                consumerRecordJavaRDD.foreachPartition( consumerRecordIterator -> {
                    while (consumerRecordIterator.hasNext()){
                        String kafkaTopic;
                        int partition;
                        String key;
                        String value;
                        long offset;
                        while (consumerRecordIterator.hasNext()){
                            ConsumerRecord<String, String> record = consumerRecordIterator.next();
                            kafkaTopic = record.topic();
                            partition = record.partition();
                            key = record.key();
                            value = record.value();
                            offset = record.offset();
                            System.out.println(kafkaTopic + "-" + partition + "-" + offset + ": " + key + ": " + value);

                            //update zookeeper
                            TopicPartition topicPartition = new TopicPartition(kafkaTopic, partition);
                            MyKafkaUtils.updateOffset(
                                    String.valueOf(kafkaParamsBroadcast.getValue().get("group.id")),
                                    topicPartition,
                                    offset + 1);
                        }
                    }
                });
            }
        });
        jssc.start();
        jssc.awaitTermination();
    }


    /**
     *
     * @param kafkaParams
     * @param topic
     * @param partitions
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void readFromKafka1(Map<String, Object> kafkaParams, String topic, Integer[] partitions)
            throws KeeperException, InterruptedException {
        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        kafkaParamsBroadcast = jssc.sparkContext().broadcast(kafkaParams);
        List<String> topics = Arrays.asList(topic);
        Map<TopicPartition, Long> offsets = MyKafkaUtils.getOffsets(
                kafkaParams.get("groupid").toString(),topic, partitions);
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferBrokers(),
                ConsumerStrategies.Subscribe(topics, kafkaParams, offsets));
        directStream.foreachRDD( consumerRecordJavaRDD -> {
            if (!consumerRecordJavaRDD.isEmpty()){
                consumerRecordJavaRDD.mapToPair( record -> new Tuple2<>(record.key(), record.value()))
                        .foreach( tuple2 -> System.out.println(tuple2._1 + ": " + tuple2._2));

                //更新offset到zookeeper
                OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();
                OffsetRange offset = offsetRanges[TaskContext.get().partitionId()];
                System.out.println(offset.topic() + ": " + offset.partition()
                        + ": " + offset.fromOffset() + ": " + offset.untilOffset());
                TopicPartition topicPartition = new TopicPartition(offset.topic(), offset.partition());
                MyKafkaUtils.updateOffset(
                        String.valueOf(kafkaParamsBroadcast.getValue().get("group.id")),
                        topicPartition, offset.untilOffset());
            }
        });

    }



}
