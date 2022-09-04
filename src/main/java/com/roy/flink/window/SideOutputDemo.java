package com.roy.flink.window;

import com.roy.flink.beans.Stock;
import com.roy.flink.streaming.FileRead;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * 演示 侧输出流，其他用法
 * @author roy
 * @date 2021/9/12
 * @desc
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final URL resource = FileRead.class.getResource("/stock.txt");
        final String filePath = resource.getFile();
//        final DataStreamSource<String> stream = env.readTextFile(filePath);
        final DataStreamSource<String> dataStream = env.readFile(new TextInputFormat(new Path(filePath)), filePath);
        final SingleOutputStreamOperator<Stock> stockStream = dataStream
                .map((MapFunction<String, Stock>) value -> {
                    final String[] split = value.split(",");
                    return new Stock(split[0], Double.parseDouble(split[1]), split[2], Long.parseLong(split[3]));
                });

        OutputTag<Stock> cheapStock = new OutputTag<Stock>("cheapStock"){};
        OutputTag<Stock> expensiveStock = new OutputTag<Stock>("expensiveStock"){};

        final SingleOutputStreamOperator<Stock> stockPriceStream = stockStream.process(new ProcessFunction<Stock, Stock>() {
            @Override
            public void processElement(Stock value, Context ctx, Collector<Stock> out) throws Exception {
                if (value.getPrice() < 10.00) {
                    ctx.output(cheapStock, value);
                } else if (value.getPrice() > 80.00) {
                    ctx.output(expensiveStock, value);
                } else {
                    out.collect(value);
                }

            }
        });

        stockPriceStream.print("stockPriceStream");
        stockPriceStream.getSideOutput(cheapStock).print("cheapStock");
        stockPriceStream.getSideOutput(expensiveStock).print("expensiveStock");

        env.execute("SideOutputDemo");
    }
}
