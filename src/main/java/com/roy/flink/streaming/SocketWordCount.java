package com.roy.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;

public class SocketWordCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        final int port = parameterTool.getInt("port");

        final DataStreamSource<String> inputDataStream = environment.socketTextStream(host, port);

        final DataStream<Tuple2<String, Integer>> wordcounts = inputDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                final String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
                .setParallelism(2)
                .keyBy(value -> value.f0)
                .sum(1)
                .setParallelism(3);
        wordcounts.print();
        wordcounts.writeToSocket(host,port,new SerializationSchema<Tuple2<String, Integer>>(){

            @Override
            public byte[] serialize(Tuple2<String, Integer> element) {
                return (element.f0+"-"+element.f1).getBytes(StandardCharsets.UTF_8);
            }
        });
        environment.execute("stream word count");
    }
}
