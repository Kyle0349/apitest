package com.kyle.spark230.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.KeeperException;

import java.util.*;


public class MyKafkaUtils {

    private static String parentPath = "/consumers/spark_kafka/offset";
    private static long initOffset = 0L;
    private static int initPartition = 3;

    /**
     * 获取offset
     * @param topic
     * @param partitions
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static Map<TopicPartition, Long> getOffset(String topic, List<Integer> partitions) throws KeeperException, InterruptedException {
        Map<TopicPartition, Long> topicPartitionLongMap = new HashMap<>();
        String zkTopicPath = parentPath + "/" + topic;
        if (!ZKUtils.isExist(zkTopicPath)){
            ZKUtils.createNode(zkTopicPath);
            for (Integer integer : partitions) {
                ZKUtils.createDataNode(zkTopicPath + "/" + integer, String.valueOf(initOffset));
            }
        }
        List<String> children = ZKUtils.getChildren(zkTopicPath);
        for (String child : children) {
            String data = ZKUtils.getZKValue(zkTopicPath + "/" + child);
            if (StringUtils.isNotBlank(data)){
                TopicPartition topicPartition = new TopicPartition(topic, Integer.valueOf(child));
                topicPartitionLongMap.put(topicPartition, Long.valueOf(data));
            }
        }
        return topicPartitionLongMap;
    }


    /**
     * 多个topic
     * @param topicPations
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static Map<TopicPartition, Long> getOffset(Map<String, List<Integer>> topicPations) throws KeeperException, InterruptedException {
        List<Map<TopicPartition, Long>> tmpList = new ArrayList<>();
        Set<String> topics = topicPations.keySet();
        for (String topic : topics) {
            Map<TopicPartition, Long> offsets = getOffset(topic, topicPations.get(topic));
            tmpList.add(offsets);
        }
        //tmpList.forEach(map -> topicPartitionLongMap.putAll(map));
        Map<TopicPartition, Long> map = MapUtils.mergeMaps(tmpList);
        return map;
    }


    /**
     * 更新zookeeper的offset
     * @param offsets
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void updateOffset(String groupid, Map<TopicPartition, Long> offsets)
            throws KeeperException, InterruptedException {
        Set<TopicPartition> topicPartitions = offsets.keySet();
        for (TopicPartition topicPartition : topicPartitions) {
            String partitionPath = parentPath + "/" + groupid + "/"
                    + topicPartition.topic() + "/" + String.valueOf(topicPartition.partition());
            ZKUtils.createOrUpdateDataNode(partitionPath, String.valueOf(offsets.get(topicPartition)));
        }
    }



    public static void updateOffset(String groupid, TopicPartition topicPartition, Long offset)
            throws KeeperException, InterruptedException {
        String partitionPath = parentPath + "/" + groupid + "/"
                + topicPartition.topic() + "/" + String.valueOf(topicPartition.partition());
        ZKUtils.createOrUpdateDataNode(partitionPath, String.valueOf(offset));
    }
}
