package com.rongxin.hadoop.lesson2;

/**
 * Created by guoxian1 on 15/3/27.
 */
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.sogou.imeda.service.IpCodeService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class LocationDis {
    public static class Map_1 extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {

        /**
         * 讲解使用生成类的变量而不是使用全局变量的重要性
         * io.sort.record.percent
         */

        IpCodeService ipcodeservice1 = null;

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            if (ipcodeservice1 == null) {
                ipcodeservice1 = new IpCodeService("location.dat",
                        "cncode.dat");
            }
            // key: lineNum, value,ip
            // System.out.println(ipcodeservice1.getLocationMap("61.51.142.1").get("cncode"));
            String ip = value.toString().split(" ", 5)[0];

            String cncode = ipcodeservice1.getLocationMap(ip.trim()).get(
                    "cncode");
            String country = ipcodeservice1.getLocationMap(ip.trim()).get(
                    "country");
            String province = ipcodeservice1.getLocationMap(ip.trim()).get(
                    "province");
            String city = ipcodeservice1.getLocationMap(ip.trim()).get("city");
            String hisen = ipcodeservice1.getLocationMap(ip.trim())
                    .get("hisen");

            if(cncode != null && province != null && province.trim().length() > 0 && !province
                    .equals("null")) {
                output.collect(new Text(province),
                        new Text("1"));
                //key : province, value: 1
            }
            /**
             * 10.10.10.1, beijing
             * 10.10.10.2, guangdong
             * 10.10.10.3, guangxi
             * 10.10.10.4, beijing
             * map output :
             * beijing 1
             * guangdong 1
             * guangxi 1
             *
             * combiner:
             * beijing 2
             * guangdong 1
             * guangxi 1
             */
        }
    }

    public static class Reduce_1 extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            // key: province ,value: [1,1,1,1]
            /**
             * beijing [2,2,2]
             */
            long sum = 0;
            while(values.hasNext()) {
                sum = sum + Long.parseLong(values.next().toString());
            }

            output.collect(key, new Text(String.valueOf(sum)));
            /**
             * beijing 6
             */
        }
    }

    /**
     * Partitioner
     *
     *
     * */
    public static class PPartition implements Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text arg1, int R) {

            /**
             * 自定义分区，实现长度不同的字符串，分到不同的reduce里面
             *
             * 现在只有3个长度的字符串，所以可以把reduce的个数设置为3
             * 有几个分区，就设置为几
             * */

            String tmpKey = key.toString();

            if(tmpKey.length()==1){
                return 1;
                //return 1%arg2; //part-00001
            }else if(tmpKey.length()==2){
                return 2;
                //return 2%arg2; //part-00002
            }else if(tmpKey.length()==3){
                return 3;
                //return 3%arg2; //part-00003
            }else if(tmpKey.length()==4){ //part-00004
                return 4;
            }else if(tmpKey.length()==5){ //part-0005
                return 5;
            }

            return 0;
        }


        @Override
        public void configure(JobConf jobConf) {

        }
    }

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(LocationDis.class);

        // password
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map_1.class);
        conf.setPartitionerClass(PPartition.class);
        conf.setCombinerClass(Reduce_1.class);
        conf.setReducerClass(Reduce_1.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

       // conf.set("fs.defaultFS", "hdfs://master:9001");

        /**
         * 讲解使用加载其他的jar 函数库的使用方式。
         */

        /**
         * 分布式缓存的使用方式。
         */

        DistributedCache.addFileToClassPath(new Path
                ("/user/hadoop1/rongxin/dis/cache/IpCnCodeService-0.0.2.jar"),conf);

        DistributedCache.addCacheFile(new URI
                        ("/user/hadoop1/rongxin/dis/cache/location" +
                        ".dat#location.dat"),
                conf);
        DistributedCache.addCacheFile(new URI("/user/hadoop1/rongxin/dis/cache/cncode.dat#cncode" +
                        ".dat"),
                conf);

        /**
         * 先不启用reduce方式，最后计算按照地域分布的情况。
         */
        conf.setNumReduceTasks(6);

        String inputDir = args[0];
        String outputDir = args[1];

        FileInputFormat.setInputPaths(conf, inputDir);
        FileOutputFormat.setOutputPath(conf, new Path(outputDir));

        boolean runFlag = JobClient.runJob(conf).isSuccessful();
        System.out.println(runFlag);

    }
}
