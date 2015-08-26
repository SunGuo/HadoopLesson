package com.rongxin.hadoop.lesson4.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * Created by guoxian1 on 15/4/2.
 */

public class IpTopK3 {

    /**
     * 第一层计算每个ip的数量
     */
    public static class IpTopKMapper1 extends Mapper<LongWritable, Text,
                Text, Text> {

        @Override
        public void map(LongWritable longWritable, Text text,Context context) throws IOException, InterruptedException {
            String ip = text.toString().split(" ", 5)[0];
            context.write(new Text(ip), new Text("1"));
        }
    }


    public static class IpTopKReducer1 extends Reducer<Text, Text,
                Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {

            long sum = 0;

            for (Text val : values) {
                sum += Long.parseLong(val.toString());
            }

            context.write(new Text(key), new Text(String.valueOf(sum)));
        }
    }

    public static class IpTopKMapper2 extends  Mapper<LongWritable, Text,
            Text, Text>{

        private static final int K = 10;

        private TreeMap<IpCountEntity, String> treeMap = new TreeMap<IpCountEntity, String>();

        @Override
        public void map(LongWritable longWritable, Text text, Context context) throws IOException {
            String [] values = text.toString().split("\t");
            /**
             * values[0] -> ip
             * values[1] -> pv
             * 10.10.10.1 100
             * 10.10.10.2 200
             * 10.10.10.3 300
             * 10.10.10.4 250
             * 10.10.10.5 249
             */

            String ip = values[0];
            String pv = values[1];
            IpCountEntity ipc = new IpCountEntity(Long.parseLong(pv), ip);

            treeMap.put(ipc, ip);

            if(treeMap.size() > K){
                treeMap.remove(treeMap.firstKey());
            }

            /**
             *
             */
        }

        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
            Iterator<IpCountEntity> iter = treeMap.keySet().iterator();

            while(iter.hasNext()){
                IpCountEntity key = iter.next();
                /**
                 *
                 * 10.10.10.1 100
                 * 10.10.10.2 200
                 * 10.10.10.3 300
                 * 10.10.10.4 250
                 * 10.10.10.5 249
                 */
                context.write(new Text(key.getIp()), new Text(String.valueOf(key.getPv())));
            //    System.out.println("k: " + key.getPv() + " v: " + key.getIp());
            }
        }
    }

    public static class IpTopkReducer2 extends Reducer<Text, Text, Text, Text>{

        private TreeMap<IpCountEntity, String> treeMap = new TreeMap<IpCountEntity, String>();
        private static final int K = 10;

        public void reduce(Text key, Iterator<Text> values, Context context){
            /**
             * key: ip, value: pv
             */

            while(values.hasNext()){
                IpCountEntity ipc = new IpCountEntity(Long.parseLong(values.next().toString()),
                        key.toString());
                treeMap.put(ipc, key.toString());
            }
        }

        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
            Iterator<IpCountEntity> iter = treeMap.keySet().iterator();

            while(iter.hasNext()){
                IpCountEntity key = iter.next();
                context.write(new Text(key.getIp()), new Text(String.valueOf(key.getPv())));
                System.out.println("k: " + key.getIp() + " v: " + key.getPv());
            }
        }

    }

    public static void main(String [] args) throws IOException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top k 2");
        //conf.set("mapred.job.tracker", "slave1:9001");

        job.setJarByClass(IpTopK3.class);

        job.setMapperClass(IpTopKMapper1.class);
        job.setCombinerClass(IpTopKReducer1.class);
        job.setReducerClass(IpTopKReducer1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String inputDir = args[0];
        String outputDir = args[1];
        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        try {
            boolean flag =  job.waitForCompletion(true);
            if(flag){
                System.out.println("run step 1 successful");
                Configuration conf1 = new Configuration();
                Job job1 = Job.getInstance(conf1, "Top k 2");
                //conf.set("mapred.job.tracker", "slave1:9001");

                job1.setJarByClass(IpTopK3.class);

                job1.setMapperClass(IpTopKMapper2.class);
                job1.setReducerClass(IpTopkReducer2.class);

                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job1, new Path(outputDir));
                FileOutputFormat.setOutputPath(job1, new Path(outputDir + "-2"));
                boolean flag1 =  job1.waitForCompletion(true);
                if(flag1){
                    System.out.println("run step 2 successful");
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
