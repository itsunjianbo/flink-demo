package com.roy.flink.transform;

import com.roy.flink.beans.Stock;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

/**
 * @author roy
 * @date 2021/9/7
 * @desc
 */
public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 4个线程
        env.setParallelism(4);

        final String filePath = BasicTransformDemo.class.getResource("/stock.txt").getFile();
        final DataStreamSource<String> dataStream = env.readTextFile(filePath, StandardCharsets.UTF_8.name());
        //将每一行记录转换成Stock对象
        final SingleOutputStreamOperator<Stock> stockStream = dataStream.map(new MapFunction<String, Stock>() {
            @Override
            public Stock map(String value) throws Exception {
                final String[] split = value.split(",");
                return new Stock(split[0],Double.parseDouble(split[1]),split[2],Long.parseLong(split[3]));
            }
        });

        final SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = stockStream.map(new MyRichFunction());
        resultStream.print("resultStream");

        env.execute("RichFunctionDemo");
    }
    public static class MyFunction implements MapFunction<Stock, Tuple2<String, Integer>>{
        @Override
        public Tuple2<String, Integer> map(Stock value) throws Exception {
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }

    // 实现自定义富函数类
    public static class MyRichFunction extends RichMapFunction<Stock, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(Stock value) throws Exception {
            //getRuntimeContext()获取当前子任务执行的上下文。每个taskManager会有一个runtimeContext
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作，一般是定义状态，或者建立数据库连接。每个slot会执行一次。
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            // 一般是关闭连接和清空状态的收尾操作。每个slot会执行一次。
            System.out.println("close");
        }
    }
}
