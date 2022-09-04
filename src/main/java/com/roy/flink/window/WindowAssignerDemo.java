package com.roy.flink.window;

import com.roy.flink.beans.Stock;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.SessionWithGap;

import java.time.Duration;

/**
 * @author roy
 * @date 2021/9/8
 * @desc
 */
public class WindowAssignerDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //如果从文件读取，数据一次就处理完了。
//        String filePath = WindowAssignerDemo.class.getResource("/stock.txt").getFile();
//        final DataStreamSource<String> dataStream = env.readTextFile(filePath, "UTF-8");
        final DataStreamSource<String> dataStream = env.socketTextStream("hadoop01", 7777);
        final SingleOutputStreamOperator<Stock> stockStream = dataStream.map(new MapFunction<String, Stock>() {
            @Override
            public Stock map(String value) throws Exception {
                final String[] split = value.split(",");
                return new Stock(split[0],Double.parseDouble(split[1]),split[2],Long.parseLong(split[3]));
            }
        });

        final TimestampAssignerSupplier<Stock> supplier = TimestampAssignerSupplier.of(new SerializableTimestampAssigner<Stock>() {
            @Override
            public long extractTimestamp(Stock element, long recordTimestamp) {
                return element.getTimestamp();
            }
        });
        final WatermarkStrategy<Stock> stockWatermarkStrategy = WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(supplier);
        stockStream.keyBy(new KeySelector<Stock, String>() {
            @Override
            public String getKey(Stock value) throws Exception {
                return value.getId();
            }
        })

//                //开窗分组
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
////                .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(5)))
////                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(15)))
//                //每个窗口只保留十条消息
//                .evictor(CountEvictor.of(10))
//                //简单聚合
                .maxBy("price").print("stockStream");
        env.execute("WindowAssignerDemo");
    }
}
