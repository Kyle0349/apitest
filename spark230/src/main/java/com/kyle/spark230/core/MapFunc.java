package com.kyle.spark230.core;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapFunc implements Serializable {


    /**
     * map 函数会被应用到RDD中每一个元素中，即RDD有多少数据量，map函数则会被调用多少次
     * 这样会消耗比较多的资源
     */
    public void map01(){
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1,2,3,4,5,6,7,8,9,10));
        JavaRDD<Integer> mapRDD = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 + 1;
            }
        });

        mapRDD.foreach( x -> System.out.println(x));

    }


    /**
     * mapPartitions 函数会被应用到RDD的每一个分区上，即RDD有多少个分区，mapPartitions就会被调用多少次
     * 优点：消耗的资源比map少
     * 缺点：可能会OOM
     */
    public void mapPartitions01(){
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1,2,3,4,5,6,7,8,9,10));

        JavaRDD<Integer> mapPartitionsRDD = rdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> integerIterator) throws Exception {
                ArrayList<Integer> rest = new ArrayList<>();
                while (integerIterator.hasNext()){
                    Integer next = integerIterator.next();
                    rest.add(next + 1);
                }
                return rest.iterator();
            }
        });
        mapPartitionsRDD.foreach( line -> System.out.println(line));
    }

    /**
     * 与mapPartitions类似，按照分区处理数据，同时多返回分区号
     */
    public void mapPartitionsWithIndex01(){
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1,2,3,4,5,6,7,8,9,10),3);

        JavaRDD<Tuple2<Integer, Integer>> tuple2JavaRDD = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Tuple2<Integer, Integer>>>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Integer partitionIndex, Iterator<Integer> v2) throws Exception {
                ArrayList<Tuple2<Integer, Integer>> rest = new ArrayList<>();
                while (v2.hasNext()){
                    Integer next = v2.next();
                    rest.add(new Tuple2<>(partitionIndex, next));
                }
                return rest.iterator();
            }
        }, false);

        tuple2JavaRDD.foreach( x -> System.out.println(x._1 + ": " + x._2));

    }


    /**
     * 获取pairsRDD中元素的value
     */
    public void mapValues01(){

        List<Tuple2<String,Integer>> list = Arrays.asList(
                new Tuple2<>("apple", 30),
                new Tuple2<>("banana", 7),
                new Tuple2<>("lemon", 4),
                new Tuple2<>("grapes", 6)
        );
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaPairRDD<String, Integer> linesRDD = jsc.parallelizePairs(list);
        JavaPairRDD<String, Integer> pairRDD = linesRDD.mapValues(v -> v.intValue());
        pairRDD.foreach( a -> System.out.println(a._1 + ": " + a._2));
    }


}
