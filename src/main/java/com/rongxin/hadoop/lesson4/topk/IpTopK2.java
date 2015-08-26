package com.rongxin.hadoop.lesson4.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by guoxian1 on 15/4/7.
 */

public class IpTopK2 {

    /**
     * 第一层计算每个ip的数量
     */

    public static class IpTopKMapper1 extends Mapper<Object, Text,
                Text, Text> {

        public void map(Object key, Text text, Context context) throws IOException,
                InterruptedException {
            String ip = text.toString().split(" ", 5)[0];
            context.write(new Text(ip), new Text("1"));
        }
    }

    public static class IpTopKReducer1 extends Reducer<Text, Text,
                Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            long sum = 0;
            for(Text o: values){
                sum = sum + Long.parseLong(o.toString());
            }
            context.write(new Text(key), new Text(String.valueOf(sum)));
        }
    }

    /**
     * 更换key 与value
     */
    public static class IpTopKMapper2 extends Mapper<Object, Text,
            LongWritable, Text> {


        public void map(Object key, Text text, Context context) throws IOException,
                InterruptedException {
           String [] ks = text.toString().split("\t");

            /**
             * mappredcue 默认的分隔符是 '\t'
             * ks[0] ->ip (key)
             * ks[1] ->sum (value)
             */

           context.write(new LongWritable(Long.parseLong(ks[1])), new Text(ks[0]));
        }
    }

    public static class IpTopKReducer2 extends  Reducer<LongWritable, Text,
            LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            /**
             * 1  10.10.10.1
             * 1  10.10.10.1
             */
            for(Text val: values){
                context.write(key, val);
            }

        }
    }

    //自定义Partition函数，此函数根据输入数据的最大值和MapReduce框架中
    //Partition的数据获取将输入数据按照大小分块的边界，然后根据输入数值和
    //边界的观关系返回对应的Partition ID

    public static class Topk2Partition extends Partitioner<LongWritable, Text> {
        @Override
        public int getPartition(LongWritable key, Text value, int numPartitins) {
            /**
             * numPartitions : 默认的分区（0 -> part-000000, 1->part-00001）
             * key : ip的访问次数
             * p : 2
             * key : 10
             * bound : 15
             *
             * key:16
             * 1*15= 15 ,  0
             * i = 1
             *  0<= k < 15
             *  return 0;
             *  i = 2;
             *  15*(2-1) =15, 15*2 = 30
             *   15<= k <30
             *   i = 3
             * 15*(3-1)=30, 15*3 =45
             * 30<= k< 45
             */

            /**
             * 1 4 3 2 7 6 5
             */

            int maxNumber = 65223;
            int bound = maxNumber / numPartitins + 1;
            long keyNumber = key.get();
            for (int i = 1; i <= numPartitins; i++) {
                if (keyNumber < bound * i && keyNumber >= bound * (i - 1)) {
                    return i - 1;
                }
            }

            return -1;
        }
    }

        public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.out.println(args.length);

        if(args.length < 2){
            System.out.println("args not right!");
            return ;
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf, "TopK2-1");
        job.setJarByClass(IpTopK2.class);
        job.setMapperClass(IpTopKMapper1.class);
        job.setReducerClass(IpTopKReducer1.class);
        job.setCombinerClass(IpTopKReducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String inputDir = args[0];
        String outputDir = args[1];

        FileInputFormat.setInputPaths(job, inputDir);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        boolean flag = job.waitForCompletion(true);

        if(flag){

            Configuration conf1 = new Configuration();
            Job job1 = new Job(conf1, "TopK2-2");
            job1.setJarByClass(IpTopK2.class);
            job1.setMapperClass(IpTopKMapper2.class);
            job1.setReducerClass(IpTopKReducer2.class);

            /**
             * 使用自定义的partion，通过实现抽象类：Partitioner。
             */

            job1.setPartitionerClass(Topk2Partition.class);

            job1.setOutputKeyClass(LongWritable.class);
            job1.setOutputValueClass(Text.class);
            job1.setNumReduceTasks(6);
            FileInputFormat.setInputPaths(job1, outputDir);
            FileOutputFormat.setOutputPath(job1, new Path(outputDir + "-2"));
            boolean flag1 = job1.waitForCompletion(true);
            if(flag1){
                System.out.println("run job-2 successful !!");
            }
        }
    }
}
