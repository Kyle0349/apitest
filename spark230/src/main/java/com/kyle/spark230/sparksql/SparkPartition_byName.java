package com.kyle.spark230.sparksql;

import jodd.util.CollectionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.Partitioner;
import org.junit.Assert;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SparkPartition_byName extends Partitioner {

    private int numPartitions;

    private Map<Object, Integer> hashCodePartitionIndexMap = new ConcurrentHashMap<Object, Integer>();

    public SparkPartition_byName(List<Object> groupList){
        int size = groupList.size();
        this.numPartitions = size;
        initMap(size, groupList);
    }

    private void initMap(int size, List<Object> groupList){
        Assert.assertTrue(CollectionUtils.isNotEmpty(groupList));
        for (int i =0; i < size; i++){
            hashCodePartitionIndexMap.put(groupList.get(i), i);
        }
    }


    @Override
    public int numPartitions() {
        return numPartitions-1;
    }

    @Override
    public int getPartition(Object key) {
        return hashCodePartitionIndexMap.get(key);
    }
}
