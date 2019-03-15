package com.kyle.spark230.sparkstreaming;

import com.kyle.spark230.utils.HbaseUtils;
import com.kyle.spark230.utils.MyKafkaUtils;
import com.kyle.spark230.utils.SparkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.apache.zookeeper.KeeperException;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;


/**
 * kafka
 */
public class KafkaSparkStreaming01 implements Serializable {

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

        //directStreamHandler(directStream);
        //ds2Hbase(directStream);
        //ds2Hbase03(directStream);
        ds2Hbase02(directStream);
        jssc.start();
        jssc.awaitTermination();
    }

    /**
     * 使用限流的方式从kafka读取数据
     *
     * 一个batch的每个分区每秒接收到的消息量=batchDuration*有效速率
     * 有效速率=取设置的maxRatePerPartition和预估的速率最小值
     * spark.streaming.kafka.maxRatePerPartition
     * @param kafkaParams
     * @param topic
     * @param partitions
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void  readFromKafkaWithSpeedControl(Map<String, Object> kafkaParams,
                                               String topic, Integer[] partitions)
            throws KeeperException, InterruptedException {
        SparkConf conf = SparkUtils.getSparkcONF();
        JavaStreamingContext jssc = SparkUtils.getStreamingContext(conf);
        kafkaParamsBroadcast = jssc.sparkContext().broadcast(kafkaParams);
        Collection<String> topics = Arrays.asList(topic);
        Map<TopicPartition, Long> offsets = MyKafkaUtils.getOffsets(
                kafkaParams.get("group.id").toString(), topic, partitions);

        DefaultPerPartitionConfig defaultPerPartitionConfig = new DefaultPerPartitionConfig(conf);
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams, offsets),
                defaultPerPartitionConfig
        );

        directStreamHandler(directStream);

        jssc.start();
        jssc.awaitTermination();
    }


    /**
     * 重分区处理kafka回来的数据
     */
    public void readFromKafkaWithRepartition(Map<String, Object> kafkaParams, String topic, Integer[] partitions)
            throws KeeperException, InterruptedException {
        SparkConf conf = SparkUtils.getSparkcONF();
        JavaStreamingContext jssc = SparkUtils.getStreamingContext(conf);
        kafkaParamsBroadcast = jssc.sparkContext().broadcast(kafkaParams);
        Collection<String> topics = Arrays.asList(topic);
        Map<TopicPartition, Long> offsets = MyKafkaUtils.getOffsets(
                kafkaParams.get("group.id").toString(), topic, partitions);

        DefaultPerPartitionConfig defaultPerPartitionConfig = new DefaultPerPartitionConfig(conf);
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams, offsets),
                defaultPerPartitionConfig
        );
        directStreamHandlerRePartition(directStream, 5);
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
     *
     */
    public void readFromKafka1(Map<String, Object> kafkaParams, String topic, Integer[] partitions)
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

        jssc.start();
        jssc.awaitTermination();

    }


    /**
     * 通过重新分区增加分区处理kafka返回来的dstream
     * @param directStream
     * @param numPartitions
     */
    private void directStreamHandlerRePartition(
            JavaInputDStream<ConsumerRecord<String, String>> directStream,
            int numPartitions){
        directStream.foreachRDD( rdd -> {

            if (!rdd.isEmpty()){
                JavaRDD<ConsumerRecord<String, String>> repartitionRdd = rdd.repartition(numPartitions);
                repartitionRdd.foreachPartition( tuples -> {
                    String kafkaTopic;
                    int partition;
                    String key;
                    String value;
                    long offset1;
                    while (tuples.hasNext()){
                        ConsumerRecord<String, String> record = tuples.next();
                        kafkaTopic = record.topic();
                        partition = record.partition();
                        key = record.key();
                        value = record.value();
                        offset1 = record.offset();
                        System.out.println(kafkaTopic + "-" + partition + "-" + offset1 + ": " + key + ": " + value);

                    }
                    System.out.println("*****************");
                });
                System.out.println("===============");
            }
        });
    }


    /**
     * 正常处理返回的dsStream
     * @param directStream
     */
    private void directStreamHandler(JavaInputDStream<ConsumerRecord<String, String>> directStream){
        //handle dataRdd
        directStream.foreachRDD(consumerRecordJavaRDD -> {
            if (!consumerRecordJavaRDD.isEmpty()){
                consumerRecordJavaRDD.foreachPartition( consumerRecordIterator -> {
                    String kafkaTopic;
                    int partition;
                    String key;
                    String value;
                    long offset1;
                    while (consumerRecordIterator.hasNext()){
                        ConsumerRecord<String, String> record = consumerRecordIterator.next();
                        kafkaTopic = record.topic();
                        partition = record.partition();
                        key = record.key();
                        value = record.value();
                        offset1 = record.offset();
                        System.out.println(kafkaTopic + "-" + partition + "-" + offset1 + ": " + key + ": " + value);

                        //update zookeeper
                        TopicPartition topicPartition = new TopicPartition(kafkaTopic, partition);
                        MyKafkaUtils.updateOffset(
                                String.valueOf(kafkaParamsBroadcast.getValue().get("group.id")),
                                topicPartition,
                                offset1 + 1);
                    }
                    System.out.println("*************");
                });
                System.out.println("==========");
            }
        });
    }


    /**
     *  使用saveAsHadoopDataset的方式将数据写入hbase
     * @param directStream
     */
    public void ds2Hbase(JavaInputDStream<ConsumerRecord<String, String>> directStream){
        directStream.foreachRDD( rdd -> {
            if (!rdd.isEmpty()){
                Connection conn = HbaseUtils.getConn();
                Configuration hConf = conn.getConfiguration();
                JobConf jobConf = new JobConf(hConf, this.getClass());
                jobConf.setOutputFormat(TableOutputFormat.class);
                jobConf.set(TableOutputFormat.OUTPUT_TABLE,"htable02");
                rdd.mapToPair( x -> {
                    Put put = new Put(Bytes.toBytes(x.key()));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(x.value()));
                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                }).saveAsHadoopDataset(jobConf);
            }
        });
    }


    /**
     * 使用put的方式 单条写入hbase
     * @param directStream
     */
    public void ds2Hbase02(JavaInputDStream<ConsumerRecord<String, String>> directStream){

        directStream.foreachRDD( rdd -> {
            if (!rdd.isEmpty()){
                rdd.foreachPartition( fp ->{
                    Connection conn = HbaseUtils.getConn();
                    Table htable03 = conn.getTable(TableName.valueOf("htable03"));
                    String kafkaTopic;
                    int partition;
                    long offset1;
                    while (fp.hasNext()){
                        ConsumerRecord<String, String> record = fp.next();
                        kafkaTopic = record.topic();
                        partition = record.partition();
                        offset1 = record.offset();
                        Put put = new Put(Bytes.toBytes(record.key()));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(record.value()));
                        htable03.put(put);

                        //update zookeeper
                        TopicPartition topicPartition = new TopicPartition(kafkaTopic, partition);
                        MyKafkaUtils.updateOffset(
                                String.valueOf(kafkaParamsBroadcast.getValue().get("group.id")),
                                topicPartition,
                                offset1 + 1);
                    }
                    htable03.close();
                    conn.close();

                });
            }
        });
    }


    /**
     * 使用put的方式 批量写入hbase
     * @param directStream
     */
    public void ds2Hbase03(JavaInputDStream<ConsumerRecord<String, String>> directStream){

        directStream.foreachRDD( rdd -> {
            if (!rdd.isEmpty()){
                rdd.foreachPartition( fp -> {
                    Connection conn = HbaseUtils.getConn();
                    Table htable04 = conn.getTable(TableName.valueOf("htable04"));
                    ArrayList<Put> puts = new ArrayList<>();
                    String kafkaTopic;
                    int partition;
                    long offset1;
                    while (fp.hasNext()){
                        ConsumerRecord<String, String> record = fp.next();
                        kafkaTopic = record.topic();
                        partition = record.partition();
                        offset1 = record.offset();
                        Put put = new Put(Bytes.toBytes(record.key()));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(record.value()));
                        puts.add(put);
                        //update zookeeper
                        TopicPartition topicPartition = new TopicPartition(kafkaTopic, partition);
                        MyKafkaUtils.updateOffset(
                                String.valueOf(kafkaParamsBroadcast.getValue().get("group.id")),
                                topicPartition,
                                offset1 + 1);
                    }
                    htable04.put(puts);
                    htable04.close();
                    conn.close();
                });
            }

        });

    }







}




















