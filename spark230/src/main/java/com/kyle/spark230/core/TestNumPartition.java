package com.kyle.spark230.core;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


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
    public void readFromCSVLocally(String filePath) throws InterruptedException {

        JavaSparkContext jsc = SparkUtils.getJsc();
        jsc.addJar("/Users/kyle/Documents/kyle/sourceCode/apitest/out/artifacts/spark_test/spark-test.jar");
        System.out.println("defaultMinPartitions： " + jsc.defaultMinPartitions()); //2
        System.out.println("defaultParallelism： " +  jsc.defaultParallelism()); //15
        JavaRDD<String> linesRdd = jsc.textFile(filePath);
        System.out.println("linesRdd： " +  linesRdd.getNumPartitions()); //5

        JavaRDD<String> repartitionRdd = linesRdd.repartition(3);

        JavaPairRDD<String, String> pairRdd = repartitionRdd.mapToPair(line -> {
            String[] split = line.split(",");
            return new Tuple2<>(split[0], line);
        });
        System.out.println("pairRdd： " +  pairRdd.getNumPartitions()); //5

        JavaPairRDD<String, Iterable<String>> groupRdd = pairRdd.groupByKey();
        System.out.println("groupRdd： " +  groupRdd.getNumPartitions());        //15
//
//
//        JavaPairRDD<String, Iterable<String>> repartition = groupRdd.repartition(5);
//        System.out.println("repartition： " +   repartition.getNumPartitions());
//
//
//        JavaPairRDD<String, Iterable<Iterable<String>>> groupByKey = repartition.groupByKey();
//        System.out.println("groupByKey： " +   groupByKey.getNumPartitions());

//        groupRdd.foreachPartition(fp -> {
//            while (fp.hasNext()){
//                Tuple2<String, Iterable<String>> next = fp.next();
//                System.out.println(next);
//            }
//        });

        //List<Tuple2<String, Iterable<String>>> collect = groupRdd.collect();
//        JavaPairRDD<String, Iterable<String>> repartition = groupRdd.repartition(20);
//        System.out.println("repartition： " +  repartition.getNumPartitions()); //20
//        JavaPairRDD<String, Iterable<Iterable<String>>> groupByKey = repartition.groupByKey();
//        System.out.println("groupByKey： " +  groupByKey.getNumPartitions());  //10

        Thread.sleep(10000000);

        /**
         * 2
         * 10
         * 5
         *
         */

    }

    public static void main(String[] args) throws InterruptedException {
        readFromCSVHdfs("hdfs://centos1:8020/tmp/test/tttts.txt");
    }
    /**
     * 从hdfs读取csv
     * 按照hdfs的block大小分区
     */
    public static void readFromCSVHdfs(String hdfsPath) throws InterruptedException {
        JavaSparkContext jsc = SparkUtils.getJscOnYarn();

        jsc.addJar("/Users/kyle/Documents/kyle/sourceCode/apitest/out/artifacts/spark_test/spark-test.jar");

        System.out.println("defaultMinPartitions: " + jsc.defaultMinPartitions());
        System.out.println("defaultParallelism: " + jsc.defaultParallelism());

        JavaRDD<String> linesRdd = jsc.textFile(hdfsPath);
        System.out.println("linesRdd: " + linesRdd.getNumPartitions());

        JavaRDD<String> coalesceRdd = linesRdd.coalesce(5);
        System.out.println("coalesce: " + coalesceRdd.getNumPartitions());


        JavaPairRDD<String, String> pairRDD = coalesceRdd.mapToPair(line -> {
            String[] split = line.split(",");
            return new Tuple2<>(split[0], line);
        });
        System.out.println("pairRDD: " + pairRDD.getNumPartitions());

        pairRDD.foreachPartition( fp -> {
            while (fp.hasNext()){
                Tuple2<String, String> next = fp.next();
                System.out.println(next._1);
            }
        });

    }


    /**
     *  测试task怎么并行读取文件
     * @param filePath
     */
    public void testNumPartition(String filePath){
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<String> linesRdd = jsc.textFile(filePath);

        System.out.println(linesRdd.getNumPartitions());

        linesRdd.foreachPartition( fp -> {

            int i =0;
            while (fp.hasNext()){
                System.out.println(i++);
                String next = fp.next();
                System.out.println(next);
                System.out.println("===========");
                Thread.sleep(1000);
            }

        });

    }


}
