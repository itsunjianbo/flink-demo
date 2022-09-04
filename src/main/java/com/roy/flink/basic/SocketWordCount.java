package com.roy.flink.basic;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;

/**
 * 简单的flink应用
 * 文本输入流
 */
public class SocketWordCount {
    public static void main(String[] args) throws Exception {

        // 获取流处理的运行环境
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 设置并行度
        environment.setParallelism(1);

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        final int port = parameterTool.getInt("port");

        // 获取数据源，读取一个test数据流
        final DataStreamSource<String> inputDataStream = environment.socketTextStream(host, port);

        final DataStream<Tuple2<String, Integer>> wordcounts = inputDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    // 拆分单词，统计单词出现的次数
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        final String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                })
                // 设置并行度
                .setParallelism(2)
                .keyBy(value -> value.f0)
                .sum(1)
                .setParallelism(1);
        // 打印到控制台
        wordcounts.print();
        environment.execute("stream word count");
    }
}
