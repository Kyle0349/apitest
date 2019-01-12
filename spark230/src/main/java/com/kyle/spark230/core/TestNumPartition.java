package com.kyle.spark230.core;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * 测试读取1g大小的数据
 * 从local读取csv
 *      结果显示textFile从local 读取文件时，会按32m大小切分分区
 * 从hdfs读取csv
 *
 * 读取mysql
 *
 *
 */
public class TestNumPartition {

    /**
     * 从本地读取csv，
     * 会按照32m大小分区
     */
    public void readFromCSVLocally(){

        JavaSparkContext jsc = SparkUtils.getJsc();

        JavaRDD<String> javaRDD = jsc.textFile("/Users/kyle/Downloads/user/raw_user.csv", 20);

        JavaRDD<String> coalesce = javaRDD.coalesce(2);

        System.out.println(coalesce.getNumPartitions());

    }


    /**
     * 从hdfs读取csv
     * 按照hdfs的block大小分区
     */
    public void readFromCSVHdfs(){
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<String> javaRDD = jsc.textFile("hdfs://centos1:8020/tmp/raw_user.csv");
        System.out.println(javaRDD.getNumPartitions());

    }


}
