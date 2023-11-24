package com.hpcc.streaminglab;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;
import org.json.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class IoTDataProcessor {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "topic_StreamingLab";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("IoTDataProcessor");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(300000)); // 5 minutes

        Set<String> topics = Collections.singleton(TOPIC_NAME);
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "iot-data-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaDStream<Double> temperatures = messages.map(record -> {
            JSONObject obj = new JSONObject(record.value());
            return obj.getDouble("temperature");
        });

        JavaDStream<Double> humidities = messages.map(record -> {
            JSONObject obj = new JSONObject(record.value());
            return obj.getDouble("humidity");
        });

        // Calculate the average value of temperature
        JavaDStream<Double> avgTemperature = temperatures
                .mapToPair(temp -> new Tuple2<>("key", new Tuple2<>(temp, 1)))
                .reduceByKeyAndWindow(
                        (x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2()),
                        new Duration(300000),
                        new Duration(300000))
                .map(pair -> pair._2()._1() / pair._2()._2());
        //// Calculate the average value of humidity
        JavaDStream<Double> avgHumidity = humidities
                .mapToPair(hum -> new Tuple2<>("key", new Tuple2<>(hum, 1)))
                .reduceByKeyAndWindow(
                        (x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2()),
                        new Duration(300000),
                        new Duration(300000))
                .map(pair -> pair._2()._1() / pair._2()._2());
        JavaDStream<Tuple2<Double, Double>> combinedStream = avgTemperature
                .mapToPair(temp -> new Tuple2<>("key", temp))
                .join(avgHumidity.mapToPair(hum -> new Tuple2<>("key", hum)))
                .map(tuple -> new Tuple2<>(tuple._2()._1(), tuple._2()._2()));

        combinedStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                Tuple2<Double, Double> avgValues = rdd.first();
                System.out.println("Avg Temperature: " + avgValues._1() + ", Avg Humidity: " + avgValues._2());
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
