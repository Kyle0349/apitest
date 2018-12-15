package com.kyle.spark230.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class SparkTest01 implements Serializable {

    public JavaRDD<String> readFromHdfs(JavaSparkContext jsc,String hdfsPath){
        JavaRDD<String> linesRdd = jsc.textFile(hdfsPath);
        linesRdd.foreachPartition( lines -> {
            while (lines.hasNext()){
                System.out.println(lines.next());
            }
        });
        return linesRdd;
    }

    public JavaRDD<String> readFromArray(JavaSparkContext jsc){

        JavaRDD<String> linesRdd = jsc.parallelize(Arrays.asList("hi hadoop", "hello spark", "hello hadoop"));
        return linesRdd;
    }


    /**
     * 充分区
     * @param jsc
     */
    public void repartition(JavaSparkContext jsc){
        JavaRDD<String> javaRDD = readFromArray(jsc);
        JavaRDD<String> result = javaRDD.repartition(5);
        result.foreachPartition( lines -> {
            while (lines.hasNext()){
                System.out.println(lines.next());
            }
            System.out.println("*************");
        });
    }




    /**
     *
     * @return
     */
    public JavaRDD<String> getUserInfoRdd(JavaSparkContext jsc){
        JavaRDD<String> userInfo = jsc.parallelize(Arrays.asList(
                "001 kyle 28 male",
                "002 truly 27 female",
                "003 jenny 26 female"));
        return userInfo;
    }


    public JavaRDD<String> getUserVisitSession(JavaSparkContext jsc){
        JavaRDD<String> userVisit = jsc.parallelize(Arrays.asList(
                "001 192.168.2.1 2018-12-10 12:21:30 phone",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "003 192.168.2.3 2018-12-10 12:21:32 phone",
                "004 192.168.2.2 2018-12-10 12:21:31 computer",
                "005 192.168.2.2 2018-12-10 12:21:31 computer",
                "006 192.168.2.2 2018-12-10 12:21:31 computer",
                "007 192.168.2.2 2018-12-10 12:21:31 computer",
                "008 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "001 192.168.2.1 2018-12-10 12:21:30 phone",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "003 192.168.2.3 2018-12-10 12:21:32 phone",
                "004 192.168.2.2 2018-12-10 12:21:31 computer",
                "005 192.168.2.2 2018-12-10 12:21:31 computer",
                "006 192.168.2.2 2018-12-10 12:21:31 computer",
                "007 192.168.2.2 2018-12-10 12:21:31 computer",
                "008 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "001 192.168.2.1 2018-12-10 12:21:30 phone",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "003 192.168.2.3 2018-12-10 12:21:32 phone",
                "004 192.168.2.2 2018-12-10 12:21:31 computer",
                "005 192.168.2.2 2018-12-10 12:21:31 computer",
                "006 192.168.2.2 2018-12-10 12:21:31 computer",
                "007 192.168.2.2 2018-12-10 12:21:31 computer",
                "008 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "001 192.168.2.1 2018-12-10 12:21:30 phone",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "003 192.168.2.3 2018-12-10 12:21:32 phone",
                "004 192.168.2.2 2018-12-10 12:21:31 computer",
                "005 192.168.2.2 2018-12-10 12:21:31 computer",
                "006 192.168.2.2 2018-12-10 12:21:31 computer",
                "007 192.168.2.2 2018-12-10 12:21:31 computer",
                "008 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer",
                "002 192.168.2.2 2018-12-10 12:21:31 computer"));
        return userVisit;
    }


}
