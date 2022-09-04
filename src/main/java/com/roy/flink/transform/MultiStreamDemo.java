package com.roy.flink.transform;

import com.roy.flink.beans.Stock;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author roy
 * @date 2021/9/7
 * @desc
 */
public class MultiStreamDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStreamSource<Integer> intStream = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
        final DataStreamSource<String> stringStream = env.fromCollection(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h"));

        final ConnectedStreams<Integer, String> connectedStreams = intStream.connect(stringStream);

        final SingleOutputStreamOperator<String> coMap = connectedStreams.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "int " + value;
            }

            @Override
            public String map2(String value) throws Exception {
                return "string " + value;
            }
        });
        coMap.print("coMap");

        final DataStreamSource<Integer> otherintStream = env.fromCollection(Arrays.asList(11, 12, 13, 14, 15, 16, 17, 18));
        final DataStream<Integer> unionStream = intStream.union(otherintStream);
        unionStream.print("unionStream");

        env.execute("MultiStreamDemo");
    }
}
