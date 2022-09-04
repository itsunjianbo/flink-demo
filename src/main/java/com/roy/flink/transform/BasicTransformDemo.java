package com.roy.flink.transform;

import com.roy.flink.beans.Stock;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;

/**
 * Transform map、flatMap、filter
 *
 * @author roy
 * @date 2021/9/7
 * @desc
 */
public class BasicTransformDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        stockStream.print("stockStream");

        //flatMap用于多层嵌套的集合。
        final SingleOutputStreamOperator<Double> priceFlatMapStream = dataStream.flatMap(new FlatMapFunction<String, Double>() {
            @Override
            public void flatMap(String value, Collector<Double> out) throws Exception {
                out.collect(Double.parseDouble(value.split(",")[1]));
            }
        });
        priceFlatMapStream.print("priceFlatMapStream");

// 上面 flatMap的另一种实现
//        final SingleOutputStreamOperator<Double> stockStream = dataStream.map(new MapFunction<String, Double>() {
//            @Override
//            public Double map(String value) throws Exception {
//                return Double.parseDouble(value.split(",")[1]);
//            }
//        });
//        stockStream.print("stockStreamSun");

        final SingleOutputStreamOperator<String> orderFilteredStream = dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("stock_2");
            }
        });
        orderFilteredStream.print("orderFilteredStream");

        env.execute("BasicTransformDemo");
    }
}
