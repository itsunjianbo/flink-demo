package com.roy.flink.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;

/**
 * Socket
 *
 * @author roy
 * @date 2021/9/7
 * @desc
 */
public class SocketSinkDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // Socket 是阻塞的，并行度只能为1
//        environment.setParallelism(1);
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        final int port = parameterTool.getInt("port");

        final DataStreamSource<String> inputDataStream = environment.socketTextStream(host, port);

        // 原来的数据流处理为新的数据流
        final DataStream<Tuple2<String, Integer>> wordcounts = inputDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        final String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .keyBy(value -> value.f0)
                .sum(1);

        // 打印
        wordcounts.print();

        // 新数据流输出
        wordcounts.writeToSocket(host, port, new SerializationSchema<Tuple2<String, Integer>>() {
            @Override
            public byte[] serialize(Tuple2<String, Integer> element) {
                return (element.f0 + "-" + element.f1).getBytes(StandardCharsets.UTF_8);
            }
        });

        environment.execute("SocketSinkDemo");
    }
}
