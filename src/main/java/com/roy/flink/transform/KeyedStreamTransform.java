package com.roy.flink.transform;

import com.roy.flink.beans.Stock;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

/**
 * map、getKey、reduce
 *
 * @author roy
 * @date 2021/9/7
 * @desc
 */
public class KeyedStreamTransform {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final String filePath = KeyedStreamTransform.class.getResource("/stock.txt").getFile();
        final DataStreamSource<String> dataStream = env.readTextFile(filePath, StandardCharsets.UTF_8.name());

        // 将每一行记录转换成Stock对象
        final SingleOutputStreamOperator<Stock> stockStream = dataStream.map(new MapFunction<String, Stock>() {
            @Override
            public Stock map(String value) throws Exception {
                final String[] split = value.split(",");
                return new Stock(split[0], Double.parseDouble(split[1]), split[2], Long.parseLong(split[3]));
            }
        });

        // 按照stock的name字段进行分组。KeySelector中，第一个泛型参数是传入的类型，第二个泛型参数是key的类型。
        final KeyedStream<Stock, String> keyedStockStream = stockStream.keyBy(new KeySelector<Stock, String>() {
            //根据记录找到分组的key。
            @Override
            public String getKey(Stock value) throws Exception {
                return value.getStockName();
            }
        });

        //keyedStockStream.print();
        //定义一个Reduce方法，将通过两两比较，获得每个stockName下价格最大的Stock。
        final SingleOutputStreamOperator<Stock> reducedStockStream = keyedStockStream.reduce(new ReduceFunction<Stock>() {
            @Override
            public Stock reduce(Stock value1, Stock value2) throws Exception {
                return value1.getPrice() > value2.getPrice() ? value1 : value2;
            }
        });

        //返回每个key中price最大的那个stock
//        reducedStockStream.print("reducedStockStream");

        final SingleOutputStreamOperator<Stock> maxRecordStream = keyedStockStream.maxBy("price");
        maxRecordStream.print("maxRecordStream");

//        //返回每个key中最大的价格
//        final SingleOutputStreamOperator<Stock> maxPriceSum = keyedStockStream.max("price");
//        maxPriceSum.print("keyedPriceSum");

        env.execute("KeyedStreamTransform");
    }

}
