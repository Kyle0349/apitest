package com.kyle.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.hive.conf.HiveConf;


public class HdfsClient {


    public void uploadData(InputStream src, String uristr, String user, String path)
            throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        URI uri = new URI(uristr);
        FileSystem fs = FileSystem.get(uri, conf, user);
        FSDataOutputStream fsDataOutputStream = fs.create(new Path(path));
        IOUtils.copyBytes(src, fsDataOutputStream, 1024, true);


    }


}
