package com.rongxin.hadoop.lessonPig;

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

/**
 * Created by guoxian1 on 15/6/6.
 */
public class pig1 {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            /**
             * \r\n\s\t
             */
            //a   1 2 3 4.2 9.8
            String [] strs = value.toString().split("\t");
            String col2 = strs[1];
            String col3 = strs[2];
            String col4 = strs[3];
            String col5 = strs[4];
            String col6 = strs[5];

            context.write(new Text(col2 + "#" + col3 + "#" + col4), new Text(col5 + "#" + col6));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            /**
             * 1#2#3, [4.2#9.8, 1.4#0.2]
             * 3#0#5, [3.5#2.1]
             * 7#9#9, [2.6#6.2, -#-]
             */
            int col5_count = 0;
            int col6_count = 0;
            int sum_col5 = 0;
            int sum_col6 = 0;
            float t5 = 0;
            float t6 = 0;
            for (Text val : values) {
                  String [] tmp = val.toString().split("#");
                  String col5 = tmp[0];
                  String col6 = tmp[1];
                  try{

                      int tmp5 =  Integer.parseInt(col5); // -
                      sum_col5 += tmp5;
                      col5_count++;
                  }catch(Exception e){

                  }

                try{
                    int tmp6 =  Integer.parseInt(col6);
                    sum_col6 += tmp6;
                    col6_count++;
                }catch(Exception e){

                }
            }
            if(col5_count != 0) {
                t5 = (float) sum_col5 / (float) col5_count;
            }

            if(col6_count != 0 ) {
                t6 = (float) sum_col6 / (float) col6_count;
            }

            context.write(key, new Text("" +t5 +"#" + t6));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9001");
        conf.set("yarn.resourcemanager.address", "master:8032");

        Job job = Job.getInstance(conf, "word count");
        //conf.set("mapred.job.tracker", "slave1:9001");

        job.setJarByClass(pig1.class);

        job.setMapperClass(TokenizerMapper.class);
       // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setNumReduceTasks(3);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
