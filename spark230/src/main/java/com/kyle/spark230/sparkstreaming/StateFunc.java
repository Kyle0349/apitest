package com.kyle.spark230.sparkstreaming;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.List;

public class StateFunc implements Serializable {

    /**
     * 计算全局词频
     * mapWithState也是spark streaming的状态管理算子，性能要比updateKeyByState高
     * @param ip
     * @param port
     * @throws InterruptedException
     */
    public void mapWithState01(String ip, int port) throws InterruptedException {

        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        jssc.checkpoint("file:///Users/kyle/Documents/tmp/spark/checkPoint/mapWithState01");
        JavaReceiverInputDStream<String> socketTextStream = jssc.socketTextStream(ip, port);

        JavaDStream<String> words = socketTextStream.flatMap(x -> Arrays.asList(x.split("\\W+")).iterator());

        JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(s -> new Tuple2<>(s, 1));

        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mapFunction =
                new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {

                    @Override
                    public Tuple2<String, Integer> call(String key, Optional<Integer> curValue, State<Integer> State) {
                        if (curValue.isPresent()){
                            if (State.exists()){
                                State.update(curValue.get() + State.get());
                            }else{
                                State.update(curValue.get());
                            }
                        }
                        return new Tuple2<String, Integer>(key, State.get());
                    }
                };

        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> ds =
                wordsDstream.mapWithState(StateSpec.function(mapFunction));

        ds.foreachRDD( rdd -> {
            if (!rdd.isEmpty()) {
                rdd.foreach( x -> System.out.println(x._1 + ": " + x._2));
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }


    /**
     * 计算两次窗口的比值
     * @param ip
     * @param port
     */
    public void mapWithState02(String ip, int port) throws InterruptedException {
        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        jssc.checkpoint("file:///Users/kyle/Documents/tmp/spark/checkPoint/mapWithState02");
        JavaReceiverInputDStream<String> socketTextStream = jssc.socketTextStream(ip, port);

        JavaPairDStream<String, Integer> wordPairs = socketTextStream
                .flatMap(line -> Arrays.asList(line.split("\\W+")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairDStream<String, Integer> rest = wordPairs
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
                }, Durations.seconds(5),Durations.seconds(5));

        Function3<String, Optional<Integer>, State<Integer>, Tuple3<String, Integer, Integer>> mapFunc =
                new Function3<String, Optional<Integer>, State<Integer>, Tuple3<String, Integer, Integer>>() {

                    @Override
                    public Tuple3<String, Integer, Integer> call(String key, Optional<Integer> curValue, State<Integer> state)
                            throws Exception {
                        Integer oldValue = 0;
                        if (curValue.isPresent()){
                            oldValue = state.exists() ? state.get() : 0;
                            state.update(curValue.get());
                        }
                        return new Tuple3<>(key, curValue.get(), oldValue);
                    }
                };


        JavaMapWithStateDStream<String, Integer, Integer, Tuple3<String, Integer, Integer>> ds =
                rest.mapWithState(StateSpec.function(mapFunc));
        ds.foreachRDD( rdd -> {

            if (!rdd.isEmpty()){

                rdd.foreach( x -> System.out.println(x._1() + "-> " + x._2() + ":" + x._3()));

            }
        });

        jssc.start();
        jssc.awaitTermination();

    }


    /**
     * 计算全局的词频
     * @param ip
     * @param port
     * @throws InterruptedException
     */
    public void updateKeyByState01(String ip, int port) throws InterruptedException {
        JavaStreamingContext jssc = SparkUtils.getStreamingContext();
        jssc.checkpoint("file:///Users/kyle/Documents/tmp/spark/checkPoint/updateKeyByState01");
        JavaReceiverInputDStream<String> socketTextStream = jssc.socketTextStream(ip, port);
        JavaPairDStream<String, Integer> words = socketTextStream.flatMap(line -> {
            String[] split = line.split("\\W+");
            return Arrays.asList(split).iterator();
        }).mapToPair(word -> new Tuple2<String, Integer>(word, 1));

        JavaPairDStream<String, Integer> res = words.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            //参数valueList：相当于这个batch，这个key的值，可能有多个，比如(hadoop,1)(hadoop,1)传入的可能是(1,1)
            //参数oldState：就是指这个key之前的状态
            @Override
            public Optional<Integer> call(List<Integer> valueList, Optional<Integer> oldState) throws Exception {
                Integer newState = 0;
                if (oldState.isPresent()) {
                    newState = oldState.get();
                }
                //更新state
                for (Integer v : valueList) {
                    newState += v;
                }
                return Optional.of(newState);
            }
        });

        res.foreachRDD( rdd -> {
            if (!rdd.isEmpty()){
                rdd.foreach( x -> System.out.println(x._1 + ": " + x._2));
            }
        });

        jssc.start();
        jssc.awaitTermination();

    }


}
