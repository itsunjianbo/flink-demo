package com.roy.flink.window;

import com.roy.flink.beans.Stock;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author roy
 * @date 2021/9/12
 * @desc Flink完整的乱序数据处理方案：
 * 1、短期迟到数据，用Watermark等待2秒
 * 2、接下来的迟到数据，用allowlateness收集数据，来一个数据重新聚合一次。
 * 3、长期迟到的数据，放到sideoutput流中，由用户定义如何处理。
 */
public class LateEventTimeDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> dataStream = env.socketTextStream("worker1", 7777);
        final SingleOutputStreamOperator<Stock> stockStream = dataStream
                .filter((str)-> StringUtils.isNotEmpty(str))
                .map(new MapFunction<String, Stock>() {
            @Override
            public Stock map(String value) throws Exception {
                final String[] split = value.split(",");
                return new Stock(split[0], Double.parseDouble(split[1]), split[2], Long.parseLong(split[3]));
            }
        });
        //KEY1：定义一个WatermarkStrategy。TODO 1.Watermark延迟2秒
        WatermarkStrategy<Stock> watermarkStrategy = WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofMillis(2))
                .withTimestampAssigner(((element, recordTimestamp) -> element.getTimestamp()));

        //注意：定义时要带后面的大括号{}。否则会报错。
        final OutputTag<Stock> stockOutputTag = new OutputTag<Stock>("lateData"){};

        final SingleOutputStreamOperator<Stock> stockoutputStream = stockStream.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy((KeySelector<Stock, String>) value -> value.getId())
                /*
                    KEY2：开窗分组 每10秒开个窗，加上上面的Watermark延迟时间，真实窗口时间为[2,12),[12,22),[22,32).....
                        也就是输入timestamp为12，22，32，42等时，会触发上一个窗口的计算。
                        窗口中包含的数据是timestamp为[0,10),[10,20),[20,30)...
                 */
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
                // TODO 2 延迟等待
                .allowedLateness(Time.seconds(1))
                // TODO 3. 侧输出
                .sideOutputLateData(stockOutputTag)
                .maxBy("price");

        stockoutputStream.print("stockStream");

        // 侧输出流
        stockoutputStream.getSideOutput(stockOutputTag).print("late");

        env.execute("EventTimeWindowDemo");
    }
    /* 测试数据：
stock_578,1,UDFStock,1
stock_578,8,UDFStock,8
stock_578,12,UDFStock,12 //watermark触发计算，统计[0,10)窗口的结果
stock_578,7,UDFStock,7 //allowlateness 触发[0,10)窗口的重新聚合
stock_578,12,UDFStock,22 //watermark触发计算，统计[10,20)窗口的结果
stock_578,7,UDFStock,7 //allowlateness 触发[0,10)窗口的重新聚合
stock_578,1012,UDFStock,1012 //水位线推高到1012
stock_578,7,UDFStock,7 //已经超出allowlateness范围，不再触发[0,10)窗口的重新聚合。重新输出到侧输出流。
     */
}
