package com.roy.flink.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Socket 数据源
 * @author roy
 * @date 2021/9/8
 * @desc
 */
public class SocketSourceDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        final int port = parameterTool.getInt("port");

        final DataStreamSource<String> inputDataStream = environment.socketTextStream(host, port);
        inputDataStream.print();

//        final DataStream<Tuple2<String, Integer>> wordcounts = inputDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                final String[] words = value.split(" ");
//                for (String word : words) {
//                    out.collect(new Tuple2<String, Integer>(word, 1));
//                }
//            }
//        })
//                .setParallelism(2)
//                .keyBy(value -> value.f0)
//                .sum(1)
//                .setParallelism(3);
//        wordcounts.print();
        environment.execute("SocketSourceDemo");
    }
}
