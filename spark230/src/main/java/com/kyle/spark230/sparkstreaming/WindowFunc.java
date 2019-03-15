package com.kyle.spark230.sparkstreaming;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;

public class WindowFunc implements Serializable
{

    /**
     * reduceByWindow 拼接或者处理指定窗口大小内的字符串
     * @param ip
     * @param port
     */
    public void reduceByWindow01(String ip, int port) throws InterruptedException {
        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        jssc.checkpoint("file:///Users/kyle/Documents/tmp/spark/checkPoint/reduceByWindow01");

        JavaReceiverInputDStream<String> socketTextStream = jssc.socketTextStream(ip, port);

        JavaDStream<String> words = socketTextStream
                .flatMap(line -> Arrays.asList(line.split("\\W+")).iterator());

        JavaDStream<String> rbw = words.reduceByWindow(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + "-" + v2;
            }
        }, Durations.seconds(5), Durations.seconds(5));

        rbw.foreachRDD( rdd -> {

            rdd.foreach( x -> System.out.println(x));

        });
        jssc.start();
        jssc.awaitTermination();

    }


    /**
     *
     * @throws InterruptedException
     */
    public void reduceByKeyAndWindow() throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Stream230Test");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(1));
        JavaReceiverInputDStream<String> inputDStream = jssc.socketTextStream("localhost", 9999);


        JavaDStream<String> words = inputDStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> rest =
                pairs.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(5), Durations.seconds(2));

        rest.foreachRDD( rdd -> rdd.foreach( res -> System.out.println(res._1 + ": " + res._2)));

        jssc.start();
        jssc.awaitTermination();

    }


    /**
     * 优化: 如果滑动间隔比滑动窗口小的时候，会出现数据重叠副本，
     * 通过checkpoint将旧窗口计算数据保存下来，后面操作只需要加新减旧即可，要比上一版本(每次都需要重新计算)更优化
     * 这样可以减少内存开销以及重新计算旧窗口数据的消耗，这时候需要使用checkpoint将旧窗口数据保存下来，
     * @throws InterruptedException
     */
    public void reduceByKeyAndWindow1() throws InterruptedException {

        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        jssc.checkpoint("file:///Users/kyle/Documents/tmp/spark/checkPoint/reduceByKeyAndWindow1");
        JavaReceiverInputDStream<String> inputDStream = jssc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = inputDStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairDStream<String, Integer> rest = pairs
                .reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 - v2;
            }
        }, Durations.seconds(6), Durations.seconds(6));

        rest.foreachRDD( rdd -> {
            if (!rdd.isEmpty()){
                rdd.foreach( res -> System.out.println(res._1 + ": " + res._2));
            }
        });
        jssc.start();
        jssc.awaitTermination();

    }

    /**
     * countByWindo: 计算指定窗口大小中接收数据的条数
     * @param ip
     * @param port
     * @throws InterruptedException
     */
    public void countByWindow01(String ip, int port) throws InterruptedException {

        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        jssc.checkpoint("file:///Users/kyle/Documents/tmp/spark/checkPoint/countByWindow01");
        JavaReceiverInputDStream<String> socketTextStream = jssc.socketTextStream(ip, port);

        JavaDStream<Long> longJavaDStream = socketTextStream.countByWindow(Durations.seconds(5), Durations.seconds(5));
        longJavaDStream.foreachRDD( rdd -> {
            rdd.foreach( x -> System.out.println(x));
        });

        jssc.start();
        jssc.awaitTermination();

    }


    /**
     *
     * @param ip
     * @param port
     */
    public void countByValueAndWindow01(String ip, int port) throws InterruptedException {

        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        jssc.checkpoint("file:///Users/kyle/Documents/tmp/spark/checkPoint/countByValueAndWindow01");
        JavaReceiverInputDStream<String> socketTextStream = jssc.socketTextStream(ip, port);

        JavaPairDStream<String, Long> count =
                socketTextStream.countByValueAndWindow(Durations.seconds(5), Durations.seconds(5));


        count.foreachRDD( rdd -> {
            rdd.foreach( x -> System.out.println(x._1 + ": " + x._2));
        });

        jssc.start();
        jssc.awaitTermination();

    }


    /**
     * @param ip
     * @param port
     * @throws InterruptedException
     */
    public void groupByKeyAndWindow01(String ip, int port) throws InterruptedException {

        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        jssc.checkpoint("file:///Users/kyle/Documents/tmp/spark/checkPoint/groupByKeyAndWindow01");
        JavaReceiverInputDStream<String> socketTextStream = jssc.socketTextStream(ip, port);

        JavaPairDStream<String, String> wordpairs = socketTextStream
                .mapToPair( line -> {
                    String[] split = line.split("\\W+");
                    return new Tuple2<>(split[0],line);
                });

        JavaPairDStream<String, Iterable<String>> rest = wordpairs.groupByKeyAndWindow(Durations.seconds(10));
        rest.foreachRDD( rdd -> {
            rdd.foreach( x -> System.out.println(x._1 + ": " + Arrays.asList(x._2)));
        });
        jssc.start();
        jssc.awaitTermination();

    }





















}
