package com.kyle.spark230.core;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class SparkTest01 implements Serializable {


    public JavaRDD<String> readFromHdfs(String hdfsPath){

        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<String> linesRdd = jsc.textFile(hdfsPath);
        linesRdd.foreachPartition(
                new VoidFunction<Iterator<String>>() {
                    public void call(Iterator<String> lines) throws Exception {
                        while (lines.hasNext()){
                            System.out.println("line: " + lines.next());
                        }
                    }
                }
        );
        return linesRdd;
    }

    public JavaRDD<String> readFromArray(){
        JavaSparkContext jsc = SparkUtils.getJsc();
        JavaRDD<String> linesRdd = jsc.parallelize(Arrays.asList("hi hadoop", "hello spark", "hello hadoop"));
        linesRdd.foreachPartition(
                new VoidFunction<Iterator<String>>() {
                    public void call(Iterator<String> lines) throws Exception {
                        while (lines.hasNext()){
                            System.out.println(lines.next());
                        }
                    }
                }
        );
        return linesRdd;
    }


}
