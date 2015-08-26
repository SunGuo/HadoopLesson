package com.rongxin.hadoop.lesson1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by guoxian1 on 15/3/21.
 */
public class demo3 {

    public static void uploadLocalFile2HDFS(String s, String d)
            throws IOException
    {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://master:9001");
        FileSystem hdfs = FileSystem.get(conf);
        Path src = new Path(s);
        Path dst = new Path(d);

        hdfs.copyFromLocalFile(src, dst);

        hdfs.close();
    }

    public static void main(String [] args){
        try {
            demo3.uploadLocalFile2HDFS("/Users/guoxian1/hadoop_cluster/teach_dir/file6.txt",
                    "/user/hadoop1/rongxin/hdfsdemo/input/");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
