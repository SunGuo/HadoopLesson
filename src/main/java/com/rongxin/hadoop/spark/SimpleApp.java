package com.rongxin.hadoop.spark;



import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by guoxian1 on 15/7/8.
 */

public class SimpleApp {

    public static void main(String[] args) {

        String logFile = "rongxin/spark/input/test.txt"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application").set("spark.cores.max",
                "10");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("xiangyang"); }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("hello"); }
        }).count();

        System.out.println("Lines with 123 : " + numAs + ", lines with hello: " + numBs);
        /**
         * Parallelized Collections
         */
        //List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        //JavaRDD<Integer> distData = sc.parallelize(data);

        /**
         * To illustrate RDD basics, consider the simple program below:
         */
        //JavaRDD<String> lines = sc.textFile("data.txt");
        //JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
        //    public Integer call(String s) { return s.length(); }
       // });

        //int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
         //   public Integer call(Integer a, Integer b) { return a + b; }
        //});

        /**
         * Passing Functions to Spark
         */

       // class GetLength implements Function<String, Integer> {
       //     public Integer call(String s) { return s.length(); }
       // }
       // class Sum implements Function2<Integer, Integer, Integer> {
       //     public Integer call(Integer a, Integer b) { return a + b; }
       // }

     //   JavaRDD<String> lines1 = sc.textFile("data.txt");
     //   JavaRDD<Integer> lineLengths1 = lines.map(new GetLength());
     //  int totalLength1 = lineLengths.reduce(new Sum());

        /**
         * Understanding closures
         */

       // int counter = 0;
       // JavaRDD<Integer> rdd = sc.parallelize(data);

// Wrong: Don't do this!!
       // rdd.foreach(x -> counter += x);

       // println("Counter value: " + counter);

        /**
         * Working with Key-Value Pairs
         */

       // JavaRDD<String> lines = sc.textFile("data.txt");
        //JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
        //JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);


    }
}
