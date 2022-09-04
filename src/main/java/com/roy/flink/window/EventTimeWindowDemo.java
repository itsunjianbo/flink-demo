package com.roy.flink.window;

import com.roy.flink.beans.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 时间语义
 * @author roy
 * @date 2021/9/10
 * @desc
 */
public class EventTimeWindowDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 设置时间语义，这个方式以及过时
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //如果设置并行度大于1，例如设为4，那么就需要从socket中读到4个超过watermark的数据，才会触发一次窗口计算。
        env.setParallelism(1);

        //如果从文件读取，数据一次就处理完了。
//        String filePath = WindowAssignerDemo.class.getResource("/stock.txt").getFile();
//        final DataStreamSource<String> dataStream = env.readTextFile(filePath, "UTF-8");
        final DataStreamSource<String> dataStream = env.socketTextStream("worker1", 7777);
        final DataStream<Stock> stockStream = dataStream.map(new MapFunction<String, Stock>() {
            @Override
            public Stock map(String value) throws Exception {
                final String[] split = value.split(",");
                return new Stock(split[0], Double.parseDouble(split[1]), split[2], Long.parseLong(split[3]));
            }
        });


        //KEY1：定义一个WatermarkStrategy。Watermark延迟2秒 TODO 1.水位线 Watermark
        WatermarkStrategy<Stock> watermarkStrategy= WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofMillis(2))
                // 时间分配器，指定时间戳字段
                .withTimestampAssigner(((element, recordTimestamp) -> element.getTimestamp()))
                .withIdleness(Duration.ofSeconds(10));

//        另一个API
//        WatermarkStrategy<Stock> watermarkStrategy= WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofSeconds(2))
//                .withTimestampAssigner(TimestampAssignerSupplier.of((element, recordTimestamp) -> element.getTimestamp()))
//                .withIdleness(Duration.ofSeconds(10));

        // assignTimestampsAndWatermarks 设置水位线
        stockStream.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy((KeySelector<Stock, String>) value -> value.getId())
                /*
                    KEY2：开窗分组 每10秒开个窗，加上上面的Watermark延迟时间，真实窗口时间为[2,12),[12,22),[22,32).....
                        也就是输入timestamp为12，22，32，42等时，会触发上一个窗口的计算。
                        窗口中包含的数据是timestamp为[0,10),[10,20),[20,30)...
                 */
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
//                .allowedLateness(Time.seconds(1)) //关闭延迟时间。
                .maxBy("price").print("stockStream");
        //已经过时的API
//        stockStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Stock>(Time.seconds(2)) {
//            @Override
//            public long extractTimestamp(Stock element) {
//                return element.getTimestamp()*1000L;
//            }
//        })

        env.execute("EventTimeWindowDemo");

        /* 测试结果：
        输入数据：
stock_578,1,UDFStock,1
stock_578,8,UDFStock,8
stock_578,12,UDFStock,12 //watermark触发计算，统计[0,10)窗口的结果
stock_578,7,UDFStock,7 //[0,10)窗口已关闭，当前数据被丢弃了。
stock_578,12,UDFStock,22 //watermark触发计算，统计[10,20)窗口的结果
stock_578,7,UDFStock,7 //[0,10)窗口已关闭，当前数据被丢弃了。
         */
    }
}
