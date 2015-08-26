package com.rongxin.hadoop.lesson4.pvuv;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by guoxian1 on 15/5/24.
 */

public class IPUv2 {

    /**
     * 使用1层mapreduce 计算 uv
     *
     */
    public static  class IpUvMapper1 extends MapReduceBase implements Mapper<LongWritable, Text,
            Text, Text> {

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String ip = text.toString().split(" ", 5)[0];
            String [] strs = text.toString().split(" ");

            outputCollector.collect(new Text("uv"), new Text(ip));
        }
    }

    public static class IpUvReducer1 extends MapReduceBase implements Reducer<Text, Text, Text,
            Text> {

        @Override
        public void reduce(Text text, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            long uv = 0;

            HashSet<String> uvSet = new HashSet<String>();

            while(iterator.hasNext()){
               uvSet.add(iterator.next().toString());
            }
            outputCollector.collect(new Text("uv"), new Text(String.valueOf(uvSet.size())));
        }
    }

    public static void main(String [] args) throws IOException {

        System.out.println(args.length);

        if(args.length < 2){
            System.out.println("args not right!");
            return ;
        }

        JobConf conf = new JobConf(IPUv2.class);
        //set output key class
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //set mapper & reducer class
        conf.setMapperClass(IpUvMapper1.class);
       // conf.setCombinerClass(IpPvUvReduce.class);
        conf.setReducerClass(IpUvReducer1.class);

        // set format
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        String inputDir = args[0];
        String outputDir = args[1];

        // FileInputFormat.setInputPaths(conf, "/user/hadoop/rongxin/locationinput/");
        FileInputFormat.setInputPaths(conf, inputDir);
        FileOutputFormat.setOutputPath(conf, new Path(outputDir));
        boolean flag = JobClient.runJob(conf).isSuccessful();
    }
}
