package com.rongxin.hadoop.lesson4.pvuv;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;


/**
 * Created by guoxian1 on 15/4/1.
 *
 * count the ip pv
 *
 */

public class IpPv1 {

    public static class IpPvUvMap extends MapReduceBase implements Mapper<LongWritable, Text,
            Text, Text> {

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text>
                outputCollector, Reporter reporter) throws IOException {
            String ip = text.toString().split(" ", 5)[0];
            outputCollector.collect(new Text("pv"), new Text("1"));
        }
    }

    public static class IpPvUvReduce extends MapReduceBase implements Reducer<Text, Text,
                Text, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text>
                outputCollector, Reporter reporter) throws IOException {

            long sum = 0;

            while(iterator.hasNext()){
                sum = sum + Long.parseLong(iterator.next().toString());
            }

            outputCollector.collect(new Text("pv"), new Text(String.valueOf(sum)));
        }
    }

    public static void main(String [] args) throws IOException {

        System.out.println(args.length);

        if(args.length < 2){
            System.out.println("args not right!");
            return ;
        }

        JobConf conf = new JobConf(IpPv1.class);
        //set output key class
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //set mapper & reducer class
        conf.setMapperClass(IpPvUvMap.class);
        conf.setCombinerClass(IpPvUvReduce.class);
        conf.setReducerClass(IpPvUvReduce.class);

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
