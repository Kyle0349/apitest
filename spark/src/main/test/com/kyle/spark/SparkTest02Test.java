package com.kyle.spark;

import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.*;

public class SparkTest02Test {

    SparkTest02 sparkTest02 = null;

    @Before
    public void instance(){
        System.setProperty("hadoop.home.dir", "E:\\virtul\\hadoop-common-2.6.0");
        sparkTest02 = new SparkTest02();

    }

    @Test
    public void readFromlocalFile() {
        sparkTest02.readFromlocalFile("file:\\F:\\tmpData\\sparkTest");
        //sparkTest02.readFromlocalFile("hdfs://cdh01:8020/tmp/sparkTest");
    }

    @Test
    public void readFromlocalFile1() {

        sparkTest02.readFromlocalFile("file:\\F:\\tmpData\\sparkTest", 5);

    }

    @Test
    public void readFromDir() {
    }

    @Test
    public void readFromDir1() {
    }

    @Test
    public void readFromHdfs() {
    }

    @Test
    public void readFromMysql() {
    }
}