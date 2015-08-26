package com.rongxin.hadoop.lesson4.topk;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by guoxian1 on 15/4/11.
 */
public class IpCount1 {

    public static class IpCounterMapper extends MapReduceBase implements Mapper<LongWritable,
                Text, Text, Text> {

        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException {
            String ip = value.toString();
            output.collect(new Text(ip), new Text("1"));
        }
    }

    public static class IpCounterReducer extends MapReduceBase implements Reducer<Text, Text,
            Text, Text>{

        @Override
        public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> output,
                           Reporter reporter) throws IOException {
            long sum = 0;
            while(iterator.hasNext()){
                sum = sum + Long.parseLong(iterator.next().toString());
            }
            output.collect(key, new Text(String.valueOf(sum)));
        }
    }

    public static void main(String [] args) throws IOException {

        /*JobConf conf = new JobConf();

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setMapperClass(IpCounterMapper.class);
        conf.setCombinerClass(IpCounterReducer.class);
        conf.setReducerClass(IpCounterReducer.class);


        String inputDir = args[0];
        String outputDir = args[1];

        FileInputFormat.setInputPaths(conf, inputDir);
        FileOutputFormat.setOutputPath(conf, new Path(outputDir));

        boolean flag = JobClient.runJob(conf).isSuccessful();

        System.out.println(args.length);*/

        if(args.length < 2){
            System.out.println("args not right!");
            return ;
        }

        JobConf conf = new JobConf(IpCount1.class);

        //set output key class
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //set mapper & reducer class
        conf.setMapperClass(IpCounterMapper.class);
        conf.setCombinerClass(IpCounterReducer.class);
        conf.setReducerClass(IpCounterReducer.class);

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
