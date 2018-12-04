package com.kyle.spark160.core;


/**
 * 在一个PairRDD或（k,v）RDD上调用，返回一个（k,Iterable<v>）。主要作用是将相同的所有的键值对分组到一个集合序列当中，
 * 其顺序是不确定的。groupByKey是把所有的键值对集合都加载到内存中存储计算，若一个键对应值太多，则易导致内存溢出。
 * groupByKey,
 *
 * 与groupByKey类似，却有不同。如(a,1), (a,2), (b,1), (b,2)。
 * groupByKey产生中间结果为( (a,1), (a,2) ), ( (b,1), (b,2) )。而reduceByKey为(a,3), (b,3)。
 * reduceByKey,
 *
 * 类似reduceByKey，对pairRDD中想用的key值进行聚合操作，
 * 使用初始值（seqOp中使用，而combOpenCL中未使用）对应返回值为pairRDD，而区于aggregate（返回值为非RDD）
 * aggregateByKey,
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
public class SparkTest04 {




}
