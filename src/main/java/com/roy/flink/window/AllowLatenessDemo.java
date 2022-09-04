package com.roy.flink.window;

import com.roy.flink.beans.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 允许等待时间
 * @author roy
 * @date 2021/9/10
 * @desc
 */
public class AllowLatenessDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> dataStream = env.socketTextStream("worker1", 7777);
        final SingleOutputStreamOperator<Stock> stockStream = dataStream.map(new MapFunction<String, Stock>() {
            @Override
            public Stock map(String value) throws Exception {
                final String[] split = value.split(",");
                return new Stock(split[0], Double.parseDouble(split[1]), split[2], Long.parseLong(split[3]));
            }
        });
        //KEY1：定义一个WatermarkStrategy。Watermark延迟2秒
        WatermarkStrategy<Stock> watermarkStrategy= WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofMillis(2))
                .withTimestampAssigner(((element, recordTimestamp) -> element.getTimestamp()));

//        另一个API
//        WatermarkStrategy<Stock> watermarkStrategy= WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofSeconds(2))
//                .withTimestampAssigner(TimestampAssignerSupplier.of((element, recordTimestamp) -> element.getTimestamp()))
//                .withIdleness(Duration.ofSeconds(10));

        stockStream.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy((KeySelector<Stock, String>) value -> value.getId())
                /*
                    KEY2：开窗分组 每10秒开个窗，加上上面的Watermark延迟时间，真实窗口时间为[2,12),[12,22),[22,32).....
                        也就是输入timestamp为12，22，32，42等时，会触发上一个窗口的计算。
                        窗口中包含的数据是timestamp为[0,10),[10,20),[20,30)...
                 */
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
                // TODO 允许等待时间
                .allowedLateness(Time.seconds(1))
                .maxBy("price").print("stockStream");

        env.execute("EventTimeWindowDemo");

        /* 测试结果：
        输入数据：
stock_578,1,UDFStock,1
stock_578,8,UDFStock,8
stock_578,12,UDFStock,12 //watermark触发计算，统计[0,10)窗口的结果
stock_578,7,UDFStock,7 //allowlateness 触发[0,10)窗口的重新聚合
stock_578,12,UDFStock,22 //watermark触发计算，统计[10,20)窗口的结果
stock_578,7,UDFStock,7 //allowlateness 触发[0,10)窗口的重新聚合
stock_578,1012,UDFStock,1011 //水位线推高到1011
stock_578,7,UDFStock,7 //allowlateness 触发[0,10)窗口的重新聚合
stock_578,1012,UDFStock,1012 //水位线推高到1012
stock_578,7,UDFStock,7 //已经超出allowlateness范围，不再触发[0,10)窗口的重新聚合。
         */
    }
}
