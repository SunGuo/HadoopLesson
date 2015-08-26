package com.rongxin.hadoop.lesson1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

/**
 * Created by guoxian1 on 15/3/22.
 */
public class demo4 {

    public static void createNewHDFSFile(String toCreateFilePath, String content) throws IOException, IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://master:9001");
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream os = hdfs.create(new Path(toCreateFilePath));
        os.write(content.getBytes("UTF-8"));

        os.close();

        hdfs.close();
    }


    public static boolean deleteHDFSFile(String dst) throws IOException
    {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://master:9001");
        FileSystem hdfs = FileSystem.get(conf);

        Path path = new Path(dst);
        boolean isDeleted = hdfs.delete(path);

        System.out.println(isDeleted);
        hdfs.close();

        return isDeleted;
    }

    public static byte[] readHDFSFile(String dst) throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://master:9001");
        FileSystem fs = FileSystem.get(conf);

        // check if the file exists
        Path path = new Path(dst);
        if ( fs.exists(path) )
        {
            FSDataInputStream is = fs.open(path);
            // get the file info to create the buffer
            FileStatus stat = fs.getFileStatus(path);

            // create the buffer
            byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
            is.readFully(0, buffer);

            is.close();
            fs.close();

            return buffer;
        }
        else
        {
            throw new Exception("the file is not found .");
        }
    }

    public static void mkdir(String dir) throws IOException
    {
        Configuration conf =  new Configuration();
        conf.set("fs.default.name", "hdfs://master:9001");
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path(dir));
        fs.close();
    }

    public static void deleteDir(String dir) throws IOException
    {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://master:9001");
        FileSystem fs = FileSystem.get(conf);

        fs.delete(new Path(dir));

        fs.close();
    }

    public static void listAll(String dir) throws IOException
    {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://master:9001");
        FileSystem fs = FileSystem.get(conf);

        FileStatus[] stats = fs.listStatus(new Path(dir));

        for(int i = 0; i < stats.length; ++i)
        {
            if (stats[i].isFile())
            {
                // regular file
                System.out.println(stats[i].getPath().toString());
            }
            else if (stats[i].isDirectory())
            {
                // dir
                System.out.println(stats[i].getPath().toString());
            }
            else if(stats[i].isSymlink())
            {
                // is s symlink in linux
                System.out.println(stats[i].getPath().toString());
            }

        }

        fs.close();
    }

    public static void main(String [] args){
        try {
           // demo4.deleteHDFSFile("/user/hadoop1/rongxin/hdfsdemo/input/file1.txt");
            //demo4.createNewHDFSFile("/user/rongxin/input/file4.txt","hello rongxin");
            //System.out.println(new String(demo4.readHDFSFile("/user/rongxin/input/file1.txt")));
            //demo4.mkdir("/user/rongxin/demo4");
            //byte [] result = demo4.readHDFSFile("/user/hadoop1/rongxin/hdfsdemo/input/file6.txt");
            //System.out.println(new String(result));
           // demo4.mkdir("/user/hadoop1/rongxin/hdfsdemo/input1/qiangge");
            //demo4.deleteDir("/user/hadoop1/rongxin/hdfsdemo/input1/qiangge");
            demo4.listAll("/user");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
