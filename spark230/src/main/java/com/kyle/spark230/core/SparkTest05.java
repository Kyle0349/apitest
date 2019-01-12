package com.kyle.spark230.core;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTest05 {


    public void getWordRdd(){

        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<String> lines = jsc.textFile("/Users/kyle/Documents/tmp/tttt.txt");
        JavaRDD<String> word = lines.map(line -> line.split(",")[3]);

        lines.coalesce(3);
        lines.repartition(3);

        word.foreachPartition( stringIterator -> {
            while (stringIterator.hasNext()){
                String next = stringIterator.next();
                System.out.println(next);
            }
        });


    }


}
