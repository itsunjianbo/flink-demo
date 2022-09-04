package com.roy.flink.streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

import java.util.Properties;

public class KafkaConsumer {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty("group.id", "test");
        final FlinkKafkaConsumer<String> mysource = new FlinkKafkaConsumer<>("flinktopic", new SimpleStringSchema(), properties);
//        mysource.setStartFromLatest();
//        mysource.setStartFromTimestamp();
        DataStream<String> stream = env.addSource(mysource);
        stream.print();
        //转存到另一个Topic
        properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        final FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>("flinktopic2"
                , new SimpleStringSchema()
                , properties
                , new FlinkFixedPartitioner<>()
                , FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                , 5);
        stream.addSink(myProducer);
        env.execute("KafkaConsumer");
    }
}
