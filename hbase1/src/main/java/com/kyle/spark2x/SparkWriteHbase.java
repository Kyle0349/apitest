package com.kyle.spark2x;

import com.kyle.utils.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

public class SparkWriteHbase {

    public JavaPairRDD spark2HbasePre(JavaSparkContext jsc) throws IOException {
        jsc.setLogLevel("WARN");
        JavaRDD<String> linesRDD = jsc.textFile("/Users/kyle/Desktop/sparkHbase.txt");
        JavaPairRDD<ImmutableBytesWritable, Put> datas = linesRDD.mapToPair(line -> {
            String[] split = line.split(",");
            String rowKey = split[0];
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(split[1]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(split[0]));
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });
        return datas;
    }


    /**
     * 使用saveAsHadoopDataset API 保存到hbase
     * @throws IOException
     */
    public void write2Hbase(JavaSparkContext jsc) throws IOException {
        JobConf jobConf = getConf("htable01");
        JavaPairRDD datas = spark2HbasePre(jsc);
        datas.saveAsHadoopDataset(jobConf);

    }


    /**
     * 使用 saveAsNewAPIHadoopDataset 保存数据到hbase
     * @throws IOException
     */
    public void write2Hbase02(JavaSparkContext jsc) throws IOException {
        Configuration conf = getConf("htable04");
        JavaPairRDD datas = spark2HbasePre(jsc);
        datas.saveAsNewAPIHadoopDataset(conf);
    }


    public JobConf getConf(String htable) throws IOException {
        Connection conn = HbaseUtils.getConn();
        Configuration hConf = conn.getConfiguration();
        JobConf jobConf = new JobConf(hConf, (this.getClass()));
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE,htable);
        return jobConf;
    }





}













