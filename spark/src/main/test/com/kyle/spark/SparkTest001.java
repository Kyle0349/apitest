package com.kyle.spark;

import com.kyle.utils.SparkUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

public class SparkTest001 implements Serializable {

    SparkTest01 sparkTest01 = null;
    private static JavaSparkContext sc = null;
    @Before
    public void instance(){
        sparkTest01 = new SparkTest01();
        sc = SparkUtils.getSparkContext();
    }


    @Test
    public void testMap(){
        sparkTest01.mapFunc(sc);
    }


    @Test
    public void testFlatMap(){
        sparkTest01.flatMapFunc(sc);
    }


    @After
    public void end(){
        sc.close();
    }




}
