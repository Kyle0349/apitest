package com.kyle.spark160.sparkStream;


import com.kyle.spark160.utils.SparkUtils;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Map$;
import scala.collection.mutable.ArrayBuffer;
import scala.util.Either;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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
    public void readFromKafka(Map<String, String> kafkaParam, Set<String> topicSet){
        JavaStreamingContext jssc = SparkUtils.getStreamingContext();


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

    private static KafkaCluster kafkaCluster = null;

    private static Broadcast<HashMap<String, String>> kafkaParamBroadcast = null;

    private static scala.collection.immutable.Set<String> immutableTopics = null;

    /**
     * spark 从kafka读取数据，并将offset保存到zk上
     */
    public void readFromKafkaWithZK(HashMap<String, String> kafkaParam, Set<String> topicSet){

        // init KafkaCluster
        kafkaCluster = getKafkaCluster(kafkaParam);
        System.out.println(kafkaCluster);

        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
        immutableTopics = mutableTopics.toSet();

        Either<ArrayBuffer<Throwable>, scala.collection.immutable.Set<TopicAndPartition>> partitions =
                kafkaCluster.getPartitions(immutableTopics);


        if( partitions.isLeft()){
            Throwable next = partitions.left().get().iterator().next();
            System.out.println("error: " + next.getMessage());
        }

        scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet2 = partitions.right().get();
        System.out.println(topicAndPartitionSet2.last().topic());


        // kafka direct stream 初始化时使用的offset数据
        Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap<TopicAndPartition, Long>();

        // 没有保存offset时（该group首次消费时）, 各个partition offset 默认为0
        if (kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).isLeft()) {
            System.out.println(kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).left().get());
            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);
            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                consumerOffsetsLong.put(topicAndPartition, 0L);
            }
        }else{
            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp =
                    kafkaCluster.getConsumerOffsets(
                            "com.kyle.test",
                            topicAndPartitionSet2).right().get();
            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);
            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);
            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                Long offset = (Long)consumerOffsets.get(topicAndPartition);
                consumerOffsetsLong.put(topicAndPartition, offset);
            }
        }

        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        kafkaParamBroadcast = jssc.sparkContext().broadcast(kafkaParam);
        // create direct stream
        JavaInputDStream<String> message = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParam,
                consumerOffsetsLong,
                new Function<MessageAndMetadata<String, String>, String>() {
                    public String call(MessageAndMetadata<String, String> v1) throws Exception {
                        return v1.message();
                    }
                }
        );


        // 得到rdd各个分区对应的offset, 并保存在offsetRanges中
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
        JavaDStream<String> javaDStream = message.transform(
                new Function<JavaRDD<String>, JavaRDD<String>>() {
                    @Override
                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                        OffsetRange[] tmpOffset = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(tmpOffset);
                        return rdd;
                    }
                }
        );



        javaDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {

                System.out.println("partitionNum: " + rdd.getNumPartitions());

                rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> stringIterator) throws Exception {
                        while (stringIterator.hasNext()){
                            System.out.println(stringIterator.next());
                        }
                    }
                });

                for (OffsetRange offsetRange : offsetRanges.get()) {

                    //封装topic.partition 与 offset对应关系 java Map
                    TopicAndPartition topicAndPartition =
                            new TopicAndPartition(offsetRange.topic(), offsetRange.partition());

                    Map<TopicAndPartition, Object> topicAndPartitionObjectMap =
                            new HashMap<TopicAndPartition, Object>();

                    topicAndPartitionObjectMap.put(topicAndPartition, offsetRange.untilOffset());

                    scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
                            javaMap2ScalaMap(topicAndPartitionObjectMap);

                    // 更新offset到kafkaCluster
                    kafkaCluster.setConsumerOffsets(kafkaParamBroadcast.getValue().get("group.id"), scalatopicAndPartitionObjectMap);
                }
            }
        });

        jssc.start();
        jssc.awaitTermination();

    }


    /**
     * javaMap 转 scalaMap
     * @param javaMap
     * @return
     */
    public static scala.collection.immutable.Map<TopicAndPartition,Object>
    javaMap2ScalaMap(Map<TopicAndPartition, Object> javaMap){
        scala.collection.mutable.Map<TopicAndPartition, Object> mapTest =
                JavaConverters.mapAsScalaMapConverter(javaMap).asScala();
        Object objTest = Map$.MODULE$.<TopicAndPartition,Object>newBuilder().$plus$plus$eq(mapTest.toSeq());
        Object resultTest = ((scala.collection.mutable.Builder) objTest).result();
        scala.collection.immutable.Map<TopicAndPartition,Object> scalaMap =
                (scala.collection.immutable.Map)resultTest;
        return scalaMap;

    }


    /**
     * 将kafkaParams转换成scala map，用于创建kafkaCluster
     *
     * @param kafkaParams kafka参数配置
     * @return kafkaCluster管理工具类
     */
    private static KafkaCluster getKafkaCluster(Map<String, String> kafkaParams) {

        scala.collection.mutable.Map<String, String> mapTest = JavaConverters.mapAsScalaMapConverter(kafkaParams).asScala();
        Object objTest = Map$.MODULE$.<String,String>newBuilder().$plus$plus$eq(mapTest.toSeq());
        Object resultTest = ((scala.collection.mutable.Builder) objTest).result();
        scala.collection.immutable.Map<String,String> scalaMap = (scala.collection.immutable.Map)resultTest;
        KafkaCluster kafkaCluster = new KafkaCluster(scalaMap);
        return kafkaCluster;

    }


}
