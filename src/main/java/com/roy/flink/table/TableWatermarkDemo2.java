package com.roy.flink.table;

import com.roy.flink.beans.Stock;
import com.roy.flink.window.WindowAssignerDemo;
import javafx.scene.control.Tab;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Table6 时间语义
 *
 * @author roy
 * @date 2021/9/13
 * @desc 在DataStream转为Table时定义事件时间。
 */
public class TableWatermarkDemo2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //如果从文件读取，数据一次就处理完了。
        String filePath = WindowAssignerDemo.class.getResource("/stock.txt").getFile();
        final DataStreamSource<String> dataStream = env.readTextFile(filePath, "UTF-8");
        final SingleOutputStreamOperator<Stock> stockStream = dataStream.map(new MapFunction<String, Stock>() {
            @Override
            public Stock map(String value) throws Exception {
                final String[] split = value.split(",");
                return new Stock(split[0], Double.parseDouble(split[1]), split[2], Long.parseLong(split[3]));
            }
        });

        // 环境设置
        final EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database").build();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);

        //KEY1：定义一个WatermarkStrategy。
        // 指定 Stock.getTimestamp() 作为1个时间戳
        // forBoundedOutOfOrderness() 乱序数据
        // TODO Watermark延迟2秒
        WatermarkStrategy<Stock> watermarkStrategy = WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofMillis(2))
                .withTimestampAssigner(((element, recordTimestamp) -> element.getTimestamp()));
        final SingleOutputStreamOperator<Stock> etStream = stockStream.assignTimestampsAndWatermarks(watermarkStrategy);

        // Watermark 将事件时间定义成一个新的字段 eventtime，其实就是 Stock.getTimestamp()
        final Table table = tableEnv.fromDataStream(etStream, $("id"), $("price"), $("stockName"), $("eventtime").rowtime());

        // 查找eventtime字段。
        final Table selectedTable = table.select($("id"), $("price"), $("eventtime"));

//        // toRetractStream 更新或者插入
//        tableEnv.toRetractStream(selectedTable, TypeInformation.of(new TypeHint<Tuple3<String, Double, Timestamp>>() {
//                }))
//                .print("selectedTable");

        // toAppendStream 全部都是新增
        tableEnv.toAppendStream(selectedTable, TypeInformation.of(new TypeHint<Tuple3<String, Double, Timestamp>>() {
                }))
                .print("selectedTable");


        // 其他，如果是 groupBy的话，只能使用更新
//        final Table selectedTable = table.groupBy($("stockName")).select($("stockName"), $("price").max().as("maxPrice"));
//        tableEnv.toRetractStream(selectedTable, TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
//        })).print("selectedTable");

        env.execute("TableWatermarkDemo2");
    }
}
