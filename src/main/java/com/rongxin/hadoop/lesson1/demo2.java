package com.rongxin.hadoop.lesson1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by guoxian1 on 15/3/21.
 */

public class demo2 {

    public static List<String> listPaths(String path) throws IOException {

        Configuration conf = new Configuration();
        String prefix_fs = "hdfs://master:9001";
        conf.set("fs.default.name", "hdfs://master:9001");
        URI uri = URI.create("hdfs://master:9001");
        FileSystem fs = FileSystem.get(conf);

        FileStatus[] statuses = fs.listStatus(new Path(path));

        List<String> files = new ArrayList<String>();
        if (statuses == null)
            return null;

        for (FileStatus status : statuses) {

            path = status.getPath().toString();
            System.out.println("AccessTime: " + status.getAccessTime());
            System.out.println("Owner: " + status.getOwner());
            System.out.println("Group: " + status.getGroup());
            System.out.println("BlockSize: " + status.getBlockSize());
            System.out.println("Len: " + status.getLen());
            System.out.println("ModificationTime: " + status.getModificationTime());
            System.out.println("Path: " + status.getPath());
            System.out.println("Permission: " + status.getPermission());
            System.out.println("Replication: " + status.getReplication());
            System.out.println("isDirectory: " + status.isDirectory());
            System.out.println("isFile: " + status.isFile());
            System.out.println("Symlink: " + status.getSymlink());

            if (path != null && path.trim().startsWith(prefix_fs)) {
                path = path.replace(prefix_fs, "");
            }
            files.add(path);
        }
        return files;
    }

    public static void  main(String [] args){
        try {
            List<String> paths = demo2.listPaths("/user/hadoop1/rongxin/input/students");

           /* for(String p: paths){
                System.out.println(p);
            }
            */
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
