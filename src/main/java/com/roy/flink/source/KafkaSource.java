package com.roy.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * kafka 数据源
 *
 * @author roy
 * @date 2021/9/7
 */
public class KafkaSource {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 指定kafka的配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "worker1:9092,worker2:9092,worker3:9092");
        properties.setProperty("group.id", "test");

        // FlinkKafkaConsumer本质就是一个Source
        // FlinkKafkaConsumer<T> ,可以解码为一个具体的pojo
        final FlinkKafkaConsumer<String> mysource = new FlinkKafkaConsumer<>("flinktopic", new SimpleStringSchema(), properties);

//        mysource.setStartFromLatest();
//        mysource.setStartFromTimestamp();

        DataStream<String> stream = env.addSource(mysource);
        stream.print();
        env.execute("KafkaConsumer");
    }
}
