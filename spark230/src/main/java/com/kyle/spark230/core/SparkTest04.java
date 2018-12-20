package com.kyle.spark230.core;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 求出PairRdd中每个key的最大值，最小值，每个key的数量统计
 *
 *  测试 aggregateByKey 算子的性能优化
 *
 */
public class SparkTest04 implements Serializable {

    List<Tuple2<String, Integer>> abk = Arrays.asList(
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class1", 4),
            new Tuple2<>("class2", 3),
            new Tuple2<>("class2", 9),
            new Tuple2<>("class2", 5),
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class1", 4),
            new Tuple2<>("class2", 3),
            new Tuple2<>("class2", 9),
            new Tuple2<>("class2", 5),
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class1", 4),
            new Tuple2<>("class2", 3),
            new Tuple2<>("class2", 9),
            new Tuple2<>("class2", 5),
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class1", 4),
            new Tuple2<>("class2", 3),
            new Tuple2<>("class2", 9),
            new Tuple2<>("class2", 5),
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class1", 4),
            new Tuple2<>("class2", 3),
            new Tuple2<>("class2", 9),
            new Tuple2<>("class2", 5),
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class1", 4),
            new Tuple2<>("class2", 3),
            new Tuple2<>("class2", 9),
            new Tuple2<>("class2", 5),
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class1", 4),
            new Tuple2<>("class2", 3),
            new Tuple2<>("class2", 9),
            new Tuple2<>("class2", 5),
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class1", 4),
            new Tuple2<>("class2", 3),
            new Tuple2<>("class2", 9),
            new Tuple2<>("class2", 5),
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class1", 4),
            new Tuple2<>("class2", 3),
            new Tuple2<>("class2", 9),
            new Tuple2<>("class2", 5),
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class1", 4),
            new Tuple2<>("class2", 3),
            new Tuple2<>("class2", 9),
            new Tuple2<>("class2", 5),
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class1", 4),
            new Tuple2<>("class2", 3),
            new Tuple2<>("class3", 9),
            new Tuple2<>("class3", 5),
            new Tuple2<>("class3", 1),
            new Tuple2<>("class3", 2),
            new Tuple2<>("class3", 4),
            new Tuple2<>("class3", 3),
            new Tuple2<>("class3", 9),
            new Tuple2<>("class3", 5),
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class1", 4),
            new Tuple2<>("class2", 3),
            new Tuple2<>("class2", 9),
            new Tuple2<>("class2", 5),
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class1", 4),
            new Tuple2<>("class2", 3),
            new Tuple2<>("class2", 9),
            new Tuple2<>("class2", 5),
            new Tuple2<>("class1", 1),
            new Tuple2<>("class1", 2),
            new Tuple2<>("class4", 4),
            new Tuple2<>("class4", 3),
            new Tuple2<>("class4", 9),
            new Tuple2<>("class4", 5),
            new Tuple2<>("class4", 5));


    /**
     *  分别求出每个key对应的最大值
     *  使用 aggregateByKey 算子
     */
    public void getBiggestValueForEachKey() throws InterruptedException {

        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaPairRDD<String, Integer> abkRDD = jsc.parallelizePairs(abk,2);

//        abkRDD.mapPartitionsWithIndex( (s, v) -> {
//            List<String> li = new ArrayList<>();
//            while (v.hasNext()) {
//                li.add("data：" + v.next() + " in " + s + " " + " partition");
//            }
//            return li.iterator();
//        }, true).foreach(m -> System.out.println(m));


        JavaPairRDD<String, Integer> resultRdd = abkRDD.aggregateByKey(0,
                (v1, v2) -> {
                    //System.out.println(v1 + ": " + v2);
                    return Math.max(v1, v2);
                },
                (v1, v2) -> Math.max(v1, v2));

        resultRdd.foreachPartition( tuples -> {
            while (tuples.hasNext()){
                Tuple2<String, Integer> result = tuples.next();
                System.out.println(result._1 + ": " + result._2);
            }

        });

        Thread.sleep(1000 * 30000);

    }


    /**
     *  分别求出每个key对应的最大值
     *  使用 aggregateByKey 算子
     */
    public void getBiggestValueForEachKey1() throws InterruptedException {

        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaPairRDD<String, Integer> abkRDD = jsc.parallelizePairs(abk,2);

        JavaPairRDD<String, Integer> resultRdd = abkRDD.reduceByKey(((v1, v2) -> Math.max(v1, v2)));

        resultRdd.foreachPartition( tuples -> {
            while (tuples.hasNext()){
                Tuple2<String, Integer> result = tuples.next();
                System.out.println(result._1 + ": " + result._2);
            }
        });

        Thread.sleep(1000 * 30000);


    }


    /**
     * 统计每个key的数量
     */
    public void countNum(){
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaPairRDD<String, Integer> abkRDD = jsc.parallelizePairs(abk,2);

        JavaPairRDD<String, Integer> resultRdd = abkRDD.mapValues(tmp -> 1).reduceByKey((v1, v2) -> v1 + v2);

        resultRdd.foreachPartition(tuple2Iterator -> {
            while (tuple2Iterator.hasNext()){
                Tuple2<String, Integer> result = tuple2Iterator.next();
                System.out.println(result._1 + ": " + result._2);
            }
        });
    }




    /**
     * groupByKey
     */
    public void groupByKey() throws InterruptedException {
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaPairRDD<String, Integer> abkRDD = jsc.parallelizePairs(abk,2);
        JavaPairRDD<String, Iterable<Integer>> groupByKeyRdd = abkRDD.groupByKey();

        groupByKeyRdd.foreachPartition( tuples -> {
            while (tuples.hasNext()){
                Tuple2<String, Iterable<Integer>> result = tuples.next();
                System.out.println(result._1 + ": " + result._2);
            }
        });

        Thread.sleep(1000*30000);
    }

}
