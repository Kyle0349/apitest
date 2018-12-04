package com.kyle.spark160.core;

/**
 * 合并两个RDD，生成一个新的RDD。实例中包含两个Iterable值，第一个表示RDD1中相同值，第二个表示RDD2中相同值（key值），
 * 这个操作需要通过partitioner进行重新分区，因此需要执行一次shuffle操作。（若两个RDD在此之前进行过shuffle，则不需要）
 * cogroup,
 *
 * 求笛卡尔乘积。该操作不会执行shuffle操作。
 * cartesian,
 *
 * 通过一个shell命令来对RDD各分区进行“管道化”。通过pipe变换将一些shell命令用于Spark中生成的新RDD
 * pipe
 *
 *
 * 重新分区，减少RDD中分区的数量到numPartitions。
 * coalesce
 *
 * repartition是coalesce接口中shuffle为true的简易实现，
 * 即Reshuffle RDD并随机分区，使各分区数据量尽可能平衡。若分区之后分区数远大于原分区数，则需要shuffle。
 * repartition
 *
 *根据partitioner对RDD进行分区，并且在每个结果分区中按key进行排序。
 * repartitionAndSortWithinPartitions
 *
 */
public class SparkTest05 {
}
