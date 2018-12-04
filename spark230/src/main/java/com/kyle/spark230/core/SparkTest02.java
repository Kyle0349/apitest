package com.kyle.spark230.core;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * 在一个PairRDD或（k,v）RDD上调用，返回一个（k,Iterable<v>）。主要作用是将相同的所有的键值对分组到一个集合序列当中，
 * 其顺序是不确定的。groupByKey是把所有的键值对集合都加载到内存中存储计算，若一个键对应值太多，则易导致内存溢出。
 * groupByKey,
 *
 * 与groupByKey类似，却有不同。如(a,1), (a,2), (b,1), (b,2)。
 * groupByKey产生中间结果为( (a,1), (a,2) ), ( (b,1), (b,2) )。而reduceByKey为(a,3), (b,3)。
 * reduceByKey,
 *
 *
 *同样是基于pairRDD的，根据key值来进行排序。ascending升序，默认为true，即升序；numTasks
 * sortByKey,
 *
 * 合并两个RDD，生成一个新的RDD。实例中包含两个Iterable值，第一个表示RDD1中相同值，第二个表示RDD2中相同值（key值），
 * 这个操作需要通过partitioner进行重新分区，因此需要执行一次shuffle操作。（若两个RDD在此之前进行过shuffle，则不需要）
 *
 * 加入一个RDD，在一个（k，v）和（k，w）类型的dataSet上调用，返回一个（k，（v，w））的pair dataSet。
 * join,
 *
 */
public class SparkTest02 implements Serializable {

    private SparkTest01 sparkTest01 = new SparkTest01();

    public JavaPairRDD<String, Integer> getPairRdd(){
        JavaRDD<String> linesRdd = sparkTest01.readFromArray();
        JavaPairRDD<String, Integer> wordsRdd = linesRdd.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String line) throws Exception {
                        String[] words = line.split(" ");
                        return Arrays.asList(words).iterator();
                    }
                }
        ).mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                }
        );
        return wordsRdd;
    }


    /**
     * groupByKey
     */
    public void groupByKey(){
        JavaPairRDD<String, Integer> pairRdd = this.getPairRdd();

        JavaPairRDD<String, Iterable<Integer>> groupByKeyRdd = pairRdd.groupByKey();
        groupByKeyRdd.foreach(
                new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
                    public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
                        System.out.println(tuple._1 + ": " + tuple._2);
                    }
                }
        );

    }

    /**
     * reduceByKey
     */
    public void reduceByKey(){
        JavaPairRDD<String, Integer> pairRdd = this.getPairRdd();
        JavaPairRDD<String, Integer> resultRdd = pairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        resultRdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> result) throws Exception {
                System.out.println(result._1 + ":" + result._2 );
            }
        });

    }

    /**
     * aggregateByKey,
     * 类似reduceByKey，对pairRDD中想用的key值进行聚合操作，
     * 使用初始值（seqOp中使用，而combOpenCL中未使用）对应返回值为pairRDD，而区于aggregate（返回值为非RDD)
     */
    public void aggregateByKey(){
        JavaPairRDD<String, Integer> pairRdd = this.getPairRdd();


    }


    /**
     *
     */
    public void sortByKey(){
        JavaPairRDD<String, Integer> pairRdd = this.getPairRdd();
        JavaPairRDD<String, Integer> pairRDD = pairRdd.sortByKey();
        pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> pair) throws Exception {
                System.out.println(pair._1 + ": " + pair._2);
            }
        });
    }





}
