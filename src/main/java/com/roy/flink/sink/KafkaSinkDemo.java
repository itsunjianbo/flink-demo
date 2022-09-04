package com.roy.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

import java.util.Properties;

/**
 * @author roy
 * @date 2021/9/7
 * @desc
 */
public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 指定kafka的配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "worker1:9092,worker2:9092,worker3:9092");
        properties.setProperty("group.id", "test");

//        FlinkKafkaConsumer本质就是一个Source
        final FlinkKafkaConsumer<String> mysource = new FlinkKafkaConsumer<>("flinktopic", new SimpleStringSchema(), properties);
//        mysource.setStartFromLatest();
//        mysource.setStartFromTimestamp();
        DataStream<String> stream = env.addSource(mysource);

        // 控制台输出
        stream.print();

        // 转存到另一个Topic
        properties = new Properties();
        properties.setProperty("bootstrap.servers", "worker1:9092,worker2:9092,worker3:9092");
        // Flink kafka制作
        final FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                // topicId
                "flinktopic2"
                // 简单字符串模式
                , new SimpleStringSchema()
                // producer 配置
                , properties
                // 选择 分区
                , new FlinkFixedPartitioner<>()
                // 容错
                , FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                // kafka 生产者池大小
                , 5);

        // 数据流输出
        stream.addSink(myProducer);
        env.execute("KafkaConsumer");
    }
}
