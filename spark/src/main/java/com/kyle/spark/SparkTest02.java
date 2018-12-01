package com.kyle.spark;

import com.kyle.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * spark read data to rdd
 */
public class SparkTest02 implements Serializable {


    private static JavaSparkContext sc = SparkUtils.getSparkContext();

    /**
     * 读取hdfs或者本地文件,默认分区
     *
     * RDD分区的一个分区原则：尽可能是得分区的个数等于集群核心数目
     *
     ** *** spark.default.parallelism ***
     * def defaultMinSplits: Int = math.min(defaultParallelism, 2)
     *
     * 无论是本地模式、Standalone模式、YARN模式或Mesos模式，
     * 我们都可以通过 *** spark.default.parallelism *** 来配置其默认分区个数
     * 若没有设置该值，则根据不同的集群环境确定该值
     *
     * 本地模式：默认为本地机器的CPU数目，若设置了local[N],则默认为N
     * Apache Mesos：默认的分区数为8
     * Standalone或YARN：默认取集群中所有核心数目的总和，或者2，取二者的较大值
     *
     *
     * hdfs: hdfs://ip:port/
     * local: file:\\
     * @param path 源文件路径
     */
    public void readFromlocalFile(String path){
        JavaRDD<String> lines = sc.textFile(path);

        System.out.println("partitonNum: " + lines.getNumPartitions());

        lines.foreach(new VoidFunction<String>() {
            public void call(String line) throws Exception {
                System.out.println(line);
            }
        });

    }

    /**
     * 读取hdfs或者本地文件，配置分区数
     *
     *
     * hdfs: hdfs://ip:port/
     *local: file:\\
     * @param path  源文件路径
     * @param patitionNum   分区数
     *
     *
     *
     */
    public void readFromlocalFile(String path, int patitionNum){
        JavaRDD<String> lines = sc.textFile(path, patitionNum);

        System.out.println("partitionNum: " + lines.getNumPartitions());

        lines.foreach(new VoidFunction<String>() {
            public void call(String line) throws Exception {
                System.out.println(line);
            }
        });

    }





    /**
     * 读取文件夹下所有文件，建议是小文件，大文件也支持，但是可能表现不佳
     * 默认分区
     * 注意：Small files are preferred, large file is also allowable, but may cause bad performance.
     * @param dirPath 源文件夹路径
     */
    public void readFromDir(String dirPath){
        JavaPairRDD<String, String> contentRdds = sc.wholeTextFiles(dirPath);

        contentRdds.foreach(new VoidFunction<Tuple2<String, String>>() {
            public void call(Tuple2<String, String> tuple2) throws Exception {
                System.out.println(tuple2._1 + ": " + tuple2._2);
            }
        });


    }

    /**
     *  读取文件夹下所有文件，建议是小文件，大文件也支持，但是可能表现不佳
     *  设置分区数
     *  注意：Small files are preferred, large file is also allowable, but may cause bad performance.
     * @param dirPath
     * @param partitionNum
     */
    public void readFromDir(String dirPath, int partitionNum){
        JavaPairRDD<String, String> contentRdds = sc.wholeTextFiles(dirPath, partitionNum);

        contentRdds.foreach(new VoidFunction<Tuple2<String, String>>() {
            public void call(Tuple2<String, String> tuple2) throws Exception {
                System.out.println(tuple2._1 + ": " + tuple2._2);
            }
        });


    }



    public void readFromHdfs(){

    }


    public void readFromMysql(){

    }




}
