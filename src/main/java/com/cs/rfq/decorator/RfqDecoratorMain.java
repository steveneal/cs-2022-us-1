package com.cs.rfq.decorator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class RfqDecoratorMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        //TODO: create a Spark configuration and set a sensible app name

        SparkConf conf = new SparkConf().setAppName("StreamRFQ");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //TODO: create a Spark streaming context
        JavaDStream<String> lines = jssc.socketTextStream("localhost", 9000);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());


        //print out the results
        words.foreachRDD(rdd -> {
            rdd.collect().forEach(line -> consume(line));
        });

        jssc.start();
        jssc.awaitTermination();
        //TODO: create a Spark session

        SparkSession session = SparkSession.builder().appName("StreamRFQ").getOrCreate();
        JavaSparkContext spark = new JavaSparkContext(session.sparkContext());

        //TODO: create a new RfqProcessor and set it listening for incoming RFQs
        RfqProcessor processor = new RfqProcessor(session, jssc);
    }
    static void consume(String line) {
        System.out.println(line);
    }

}
