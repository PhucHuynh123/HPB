package com.hpcc.streaminglab;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Serializable;
import org.apache.hadoop.mapred.TextOutputFormat;
import scala.Tuple2;

public class WordCount {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "topic_2014160";
    private static final String HDFS_OUTPUT_PATH = "hdfs://localhost:9000/user/Admin/output_folder";
    private static final int WORDS_TO_WRITE = 100; // Số lượng từ trước khi lưu file

    public static void main(String[] args) throws Exception {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));
        jssc.sparkContext().setLogLevel("WARN");
        jssc.checkpoint("/tmp/2014160");

        Collection<String> topics = Arrays.asList(TOPIC_NAME);

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaPairDStream<String, Integer> wordCounts = stream
                .flatMap(record -> Arrays.asList(record.value().split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);

        // Sử dụng biến đếm để kiểm soát số lượng từ
        wordCounts.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                rdd.mapToPair((PairFunction<Tuple2<String, Integer>, String, String>)
                pair -> new Tuple2<>(pair._1() + ": ", String.valueOf(pair._2())))
                .saveAsHadoopFile(
                        HDFS_OUTPUT_PATH + "/output_" + System.currentTimeMillis(),
                        String.class,
                        String.class,
                        TextOutputFormat.class
                );
            }
        });
        jssc.start();
        jssc.awaitTermination();
    }
}