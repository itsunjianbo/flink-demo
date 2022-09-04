package com.roy.flink.window;

import com.roy.flink.beans.Stock;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.netty4.io.netty.util.internal.StringUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author roy
 * @date 2021/9/10
 * @desc
 */
public class WatermarkDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //每10秒自动提交一次watermark
        env.getConfig().setAutoWatermarkInterval(10*1000);
        env.setParallelism(1);//SocketStrema只能用一个Slot 。
        final DataStreamSource<String> dataStream = env.socketTextStream("worker1", 7777);
        final SingleOutputStreamOperator<Stock> stockStream = dataStream
                .filter((str)-> !StringUtil.isNullOrEmpty(str))
                .map((MapFunction<String, Stock>) value -> {
                    final String[] split = value.split(",");
                    return new Stock(split[0], Double.parseDouble(split[1]), split[2], Long.parseLong(split[3]));
                });
        //定义一个WatermarkStrategy
        WatermarkStrategy<Stock> watermarkStrategy=  WatermarkStrategy.forGenerator((ctx)-> new MyWatermarkGenerator())
                .withTimestampAssigner(((element, recordTimestamp) -> element.getTimestamp()));

//        WatermarkStrategy<Stock> watermarkStrategy= WatermarkStrategy.<Stock>forMonotonousTimestamps()
//                .withTimestampAssigner(((element, recordTimestamp) -> element.getTimestamp()));
        stockStream.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy((KeySelector<Stock, String>) value -> value.getId())
                //开窗分组
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
//                .allowedLateness(Time.milliseconds(5))
                .maxBy("price").print("stockStream");

        env.execute("WatermarkDemo");
    }

    // TODO 自定义 Watermark
    public static class MyWatermarkGenerator implements WatermarkGenerator<Stock>{

        @Override
        public void onEvent(Stock event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("MyWatermarkGenerator.onEvent"+event+" > "+eventTimestamp);
            output.emitWatermark(new Watermark(event.getTimestamp()));
        }

        // 每隔10秒提交一次
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("MyWatermarkGenerator.onPeriodicEmit");
        }
    }
}
