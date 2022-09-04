package com.roy.flink.source;

import com.roy.flink.beans.Stock;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 自定义Source
 * @author roy
 * @date 2021/9/7
 * @desc 自定义Source
 */
public class UDFSource {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 添加一个Source
        final DataStreamSource<Stock> orderDataStreamSource = env.addSource(new MyOrderSource());
        orderDataStreamSource.print();
        env.execute("UDFOrderSOurce");
    }


    // 自定义的Source 实现 SourceFunction 或者 RichSourceFunction

    /**
     * 自定义的Source
     */
    public static class MyOrderSource implements SourceFunction<Stock> {
        private boolean running = true;

        @Override
        public void run(SourceContext<Stock> ctx) throws Exception {
            final Random random = new Random();
            while (running) {
                // 创建数据
                Stock stock = new Stock();
                stock.setId("stock_" + System.currentTimeMillis() % 700);
                stock.setPrice(random.nextDouble() * 100);
                stock.setStockName("UDFStock");
                stock.setTimestamp(System.currentTimeMillis());

                // 收集数据
                ctx.collect(stock);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }


//    public static class MyOrderSource1 implements SourceFunction<Stock> {
//
//        @Override
//        public void run(SourceContext<Stock> sourceContext) throws Exception {
//
//        }
//
//        @Override
//        public void cancel() {
//
//        }
//    }

}
