package com.rongxin.hadoop.lesson1;

/**
 * Created by guoxian1 on 15/3/21.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class wordcount2 {
        public static class TokenizerMapper
                extends Mapper<Object, Text, Text, IntWritable>{

            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();


            public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {

                /**
                 * \r\n\s\t
                 */
                /**
                 * hello world
                 * hello rongxin
                 * hello zgx
                 */
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                    String tmpValue = itr.nextToken();
                    word.set(tmpValue);
                    if(tmpValue.trim().equals("rongxin")){
                        context.write(word, one);
                    }
                }
                /**
                 *  no combiner
                 * hello 1
                 * hello 1
                 * hello 1
                 */

                /**
                 * has combiner
                 * hello 3
                 */
            }
        }

        public static class IntSumReducer
                extends Reducer<Text,IntWritable,Text,IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(Text key, Iterable<IntWritable> values,
                               Context context
            ) throws IOException, InterruptedException {
                int sum = 0;
                // {"hello", [1, 1,1,1]}
                for (IntWritable val : values) {
                    sum += val.get();
                }

                result.set(sum);
                context.write(key, result);
            }
        }

        public static void main(String[] args) throws Exception {

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://master:9001");
            conf.set("yarn.resourcemanager.address", "master:8032");

            Job job = Job.getInstance(conf, "word count");
            //conf.set("mapred.job.tracker", "slave1:9001");

            job.setJarByClass(wordcount2.class);

            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);

            job.setNumReduceTasks(3);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
