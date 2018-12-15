package com.kyle.spark230.core;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

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
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<String> linesRdd = sparkTest01.readFromArray(jsc);
        JavaPairRDD<String, Integer> wordsRdd = linesRdd
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1));
        return wordsRdd;
    }


    /**
     * groupByKey
     */
    public void groupByKey(){
        JavaPairRDD<String, Integer> pairRdd = this.getPairRdd();

        JavaPairRDD<String, Iterable<Integer>> groupByKeyRdd = pairRdd.groupByKey();
        groupByKeyRdd.foreachPartition(tuples -> {
            while (tuples.hasNext()){
                Tuple2<String, Iterable<Integer>> tuple = tuples.next();
                System.out.println(tuple._1 + ": " + tuple._2);
            }
        });
    }

    /**
     * 通过提高reduce 端并行度 环节数据倾斜问题
     * reduceByKey
     */
    public void reduceByKey() throws InterruptedException {
        JavaPairRDD<String, Integer> pairRdd = this.getPairRdd();

        //JavaPairRDD<String, Integer> resultRdd = pairRdd.reduceByKey((v1, v2) -> v1 + v2);
        //提升shuffle reduce 端并行度
        JavaPairRDD<String, Integer> resultRdd = pairRdd.reduceByKey((v1, v2) -> v1 + v2, 10);

        resultRdd.foreachPartition( tuples -> {
            while (tuples.hasNext()){
                Tuple2  result = tuples.next();
                System.out.println(result._1 + ": " + result._2);
            }
            System.out.println("===============");
        });



        Thread.sleep(1000*3000);
    }

    /**
     * 通过给key加盐的方式解决数据倾斜问题 二次reduce
     */
    public void reduceByKey2(){

        JavaPairRDD<String, Integer> pairRdd = this.getPairRdd();
        //给key加盐
        JavaPairRDD<String, Integer> stage1Rdd = pairRdd.mapToPair(pair -> {
            Random random = new Random();
            int prefix = random.nextInt(10);
            return new Tuple2<>(prefix + "_" + pair._1, pair._2);
        });

        JavaPairRDD<String, Integer> stage2Rdd = stage1Rdd.reduceByKey((v1, v2) -> v1 + v2);

        stage2Rdd.foreachPartition( tuples -> {
            while (tuples.hasNext()){
                Tuple2<String, Integer> result = tuples.next();
                System.out.println(result._1 + ": " + result._2);
            }
            System.out.println("==========4=======");
        });

        //去除掉key的盐
        JavaPairRDD<String, Integer> stage3Rdd = stage2Rdd.mapToPair(pair -> {
            String key = pair._1.split("_")[1];
            return new Tuple2<>(key, pair._2);
        });

        //全局shuffle
        JavaPairRDD<String, Integer> result1Rdd = stage3Rdd.reduceByKey((v1, v2) -> v1 + v2);


        result1Rdd.foreachPartition( tuples -> {
            while (tuples.hasNext()){
                Tuple2<String, Integer> result = tuples.next();
                System.out.println(result._1 + ": " + result._2);
            }
            System.out.println("=================");
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
     * 根据key排序
     */
    public void sortByKey(){
        JavaPairRDD<String, Integer> pairRdd = this.getPairRdd();
        JavaPairRDD<String, Integer> sortRdd = pairRdd.sortByKey();
        sortRdd.foreachPartition( tuples -> {
            while (tuples.hasNext()){
                Tuple2<String, Integer> result = tuples.next();
                System.out.println(result._1 + ": " + result._2);
            }
        });
    }




    public void reduceJoin(){
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<String> userInfoRdd = sparkTest01.getUserInfoRdd(jsc);
        JavaRDD<String> userVisitSession = sparkTest01.getUserVisitSession(jsc);
        JavaPairRDD<String, String> userInfoPairRdd = userInfoRdd.mapToPair(line -> {
            String[] split = line.split(" ");
            return new Tuple2<>(split[0], line);
        });

        JavaPairRDD<String, String> userVisitPairRdd = userVisitSession.mapToPair(line -> {
            String[] split = line.split(" ");
            return new Tuple2<>(split[0], line);
        });


        JavaPairRDD<String, Tuple2<String, String>> join = userVisitPairRdd.join(userInfoPairRdd);

        join.foreach( tuple -> {
            System.out.println(tuple._1 + ": " + tuple._2._1 + ": " + tuple._2._2);
        });


    }


    /**
     * 当两个表join时，如果有一个表时小表，每个executor能够存下这个表，则可以将这张表读进内存
     * 并广播给出去给每个executor驻留一份，
     * 这样可以将reduce join 转化为map join，不会发生shuffle
     */
    public void mapJoin(){
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<String> userInfoRdd = sparkTest01.getUserInfoRdd(jsc);
        JavaRDD<String> userVisitSession = sparkTest01.getUserVisitSession(jsc);
        //生成pairRdd
        JavaPairRDD<String, String> userInfoPairRdd = userInfoRdd.mapToPair(line -> {
            String[] split = line.split(" ");
            return new Tuple2<>(split[0], line);
        });

        JavaPairRDD<String, String> userVisitPairRdd = userVisitSession.mapToPair(line -> {
            String[] split = line.split(" ");
            return new Tuple2<>(split[0], line);
        });

        //将小表设置为广播变量
        List<Tuple2<String, String>> collect = userInfoPairRdd.collect();
        final Broadcast<List<Tuple2<String, String>>> broadcastUserInfo = jsc.broadcast(collect);

        //用大表join小表
        JavaPairRDD<String, Tuple2<String, String>> resultRdd = userVisitPairRdd.mapToPair(pair -> {
            //得到用户信息的map
            List<Tuple2<String, String>> userInfos = broadcastUserInfo.value();
            Map<String, String> userInfoMap = new HashMap<>();
            for (Tuple2<String, String> userInfo : userInfos) {
                userInfoMap.put(userInfo._1, userInfo._2);
            }
            //获取当前用户对应的信息
            String userInfo = userInfoMap.get(pair._1);
            String userVisitSess = pair._2;
            return new Tuple2<>(pair._1, new Tuple2<>(userVisitSess, userInfo));
        }).filter( tuple2 -> tuple2._2._2 !=null);


        resultRdd.foreach( tuple2 -> {
            System.out.println(tuple2._1 + ": " + tuple2._2._1 + ": " + tuple2._2._2);
        });

    }


    /**
     * sample 采样倾斜key单独进行join
     */
    public void sample(){
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<String> userInfoRdd = sparkTest01.getUserInfoRdd(jsc);
        JavaRDD<String> userVisitSession = sparkTest01.getUserVisitSession(jsc);
        JavaPairRDD<String, String> userInfoPairRdd = userInfoRdd.mapToPair(line -> {
            String[] split = line.split(" ");
            return new Tuple2<>(split[0], line);
        });

        JavaPairRDD<String, String> userVisitPairRdd = userVisitSession.mapToPair(line -> {
            String[] split = line.split(" ");
            return new Tuple2<>(split[0], line);
        });


        //userVisitPairRdd.foreach( tuple2 -> System.out.println(tuple2._1 + "=" + tuple2._2));

        //采样获取对应数据量最大的key
        JavaPairRDD<String, String> sampleRdd = userVisitPairRdd.sample(false, 0.1, 99);

        //sampleRdd.foreach( tuple2 -> System.out.println(tuple2._1 + "-" + tuple2._2));

        JavaPairRDD<String, Long> mapSampleRdd = sampleRdd.mapToPair(tuple2 -> new Tuple2<>(tuple2._1, 1L));
        JavaPairRDD<String, Long> reduceRdd = mapSampleRdd.reduceByKey((v1, v2) -> v1 + v2);
        JavaPairRDD<Long, String> reverseSampleRdd = reduceRdd.mapToPair(pair -> new Tuple2<>(pair._2, pair._1));
        JavaPairRDD<Long, String> longStringJavaPairRDD = reverseSampleRdd.sortByKey();
        longStringJavaPairRDD.foreach( longStringTuple2 -> System.out.println(longStringTuple2._1 + ": " + longStringTuple2._2));
        final String key = reverseSampleRdd.sortByKey(false).take(1).get(0)._2;

        JavaPairRDD<String, Tuple2<String, String>> skwedJoinRdd =
                userVisitPairRdd.filter(tuple2 -> tuple2._1.equals(key)).join(userInfoPairRdd);

        //获取到正常keyRdd
        JavaPairRDD<String, String> commonRdd = userVisitPairRdd.filter(tuple2 -> !tuple2._1.equals(key));
        JavaPairRDD<String, Tuple2<String, String>> joinRdd2 = commonRdd.join(userInfoPairRdd);
        //System.out.println("2num: " + joinRdd2.getNumPartitions());

        //合并jion结果表
        JavaPairRDD<String, Tuple2<String, String>> unionRdd = skwedJoinRdd.union(joinRdd2);
        unionRdd.foreach( tuple2 -> System.out.println(tuple2._1 + ": " + tuple2._2._1 + ": " + tuple2._2._2));

    }


}
