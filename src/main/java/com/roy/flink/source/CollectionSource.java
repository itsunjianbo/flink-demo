package com.roy.flink.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * 集合数据源
 *
 * @author roy
 * @date 2021/9/7
 */
public class CollectionSource {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        final DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5);

        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final DataStreamSource<Integer> stream = env.fromCollection(list);

        stream.print();
        final SingleOutputStreamOperator<Integer> stream2 = stream.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        });
        stream2.print();
        final DataStream<Integer> union = stream.union(stream2);
        env.execute("CollectionSink");
    }
}
