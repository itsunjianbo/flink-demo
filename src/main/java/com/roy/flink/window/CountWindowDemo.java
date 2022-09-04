package com.roy.flink.window;

import com.roy.flink.beans.Stock;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author roy
 * @date 2021/9/8
 * @desc
 */
public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String filePath = CountWindowDemo.class.getResource("/stock.txt").getFile();
        final DataStreamSource<String> dataStream = env.readTextFile(filePath, "UTF-8");
//        final DataStreamSource<String> dataStream = env.socketTextStream("localhost", 7777);
        final SingleOutputStreamOperator<Stock> stockStream = dataStream.map(new MapFunction<String, Stock>() {
            @Override
            public Stock map(String value) throws Exception {
                final String[] split = value.split(",");
                return new Stock(split[0],Double.parseDouble(split[1]),split[2],Long.parseLong(split[3]));
            }
        });
        final SingleOutputStreamOperator<Stock> maxPriceStock = stockStream.keyBy(new KeySelector<Stock, String>() {
            @Override
            public String getKey(Stock value) throws Exception {
                return value.getId();
            }
        })
                //size:窗口长度，slide:起始点的偏移量
                .countWindow(5, 2)
                //获得股票价值最高的那一条记录。
                .maxBy("price");
        maxPriceStock.print("maxPriceStock");
        env.execute("CountWindowDemo");
    }
}
