package com.roy.flink.window;

import com.roy.flink.beans.Stock;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author roy
 * @date 2021/9/8
 * @desc
 */
public class WindowFunctionDemo {
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
                return new Stock(split[0], Double.parseDouble(split[1]), split[2], Long.parseLong(split[3]));
            }
        });
        stockStream.keyBy(new KeySelector<Stock, String>() {
            @Override
            public String getKey(Stock value) throws Exception {
                return value.getId();
            }
        })
                //开窗分组
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(5)))
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(15)))
                //每个窗口只保留十条消息
//                    .evictor(CountEvictor.of(10))
                //简单聚合
//                    .maxBy("price").print("stockStream");
                //自定义聚合 求每个窗口内的股票平均价格
                .aggregate(new MyAvgFunction()).print("stockStream");
                // 自定义全窗口聚合。之前的全窗口类型只有一个结果，不知道是哪个stock，自定义的示例就可以加上sotkcId。
                // WindowFunction的四个泛型依次表示： 传入数据类型、返回结果类型、key类型、窗口类型。
//                .apply(new WindowFunction<Stock, Tuple2<String,Integer>, String, TimeWindow>() {
//                    //四个参数依次表示：当前数据的key，当前窗口类型，当前窗口内所有数据的迭代器、输出结果收集器
//                    @Override
//                    public void apply(String s, TimeWindow window, Iterable<Stock> input, Collector<Tuple2<String,Integer>> out) throws Exception {
//                        final int count = IteratorUtils.toList(input.iterator()).size();
//                        out.collect(new Tuple2<>(s,count));
//                    }
//                }).print("stockStream");
        env.execute("WindowAssignerDemo");
    }
    //AggragationFunction的三个泛型依次表示：传入数据类型、累加器的类型、输出数据类型
    public static class MyAvgFunction implements AggregateFunction<Stock, Tuple2<Double, Integer>, Double> {
        //创建一个累加器，初始值
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        //在累加器上添加一个元素
        @Override
        public Tuple2<Double, Integer> add(Stock value, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.getPrice(), accumulator.f1 + 1);
        }

        //返回最终的结果
        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        //将两个累加器合并到一起，主要用于分区合并
        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
