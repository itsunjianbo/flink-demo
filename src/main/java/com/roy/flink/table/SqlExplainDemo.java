package com.roy.flink.table;

import com.roy.flink.beans.Stock;
import com.roy.flink.streaming.FileRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Table7 执行计划
 *
 * @author roy
 * @date 2021/9/13
 * @desc
 */
public class SqlExplainDemo {
    public static void main(String[] args) throws Exception {
        //1、读取数据
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

        //2、创建StreamTableEnvironment catalog -> database -> tablename
        final EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database").build();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
//        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3、基于流创建表
        final Table stockTable = tableEnv.fromDataStream(stockStream);

        //4、使用SQL查询
        tableEnv.createTemporaryView("stock", stockTable);

        //简单的SQL
        String sql = "select id,stockName from stock";

        //稍微复杂一点的SQL
//        String sql = "select id,stockName,avg(price) as priceavg from stock where stockName='UDFStock' group by id,stockName";

        final String explaination = tableEnv.explainSql(sql);
        System.out.println(explaination);

        env.execute("SqlExplainDemo");
    }
}
