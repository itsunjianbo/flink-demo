package com.roy.flink.table;

import com.roy.flink.beans.Stock;
import com.roy.flink.streaming.FileRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.net.URL;

/**
 * table2 永久表实例
 *
 * @author roy
 * @date 2021/9/12
 * @desc
 */
public class PermanentFileTableDemo {
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

        // 创建了1个表，输出的格式为csv
        String sql = "create table stock(" +
                "            id varchar," +
                "            price double," +
                "            stockName varchar," +
                "            `timestamp` bigint" +
                "          ) with (" +
                "            'connector.type' = 'filesystem'," +
                "            'format.type' = 'csv'," +
                "            'connector.path' = 'D://flinktable'" +
                "          )";
        tableEnv.executeSql(sql);

//        // TODO 创建临时表。计算任务结束时，表就会回收。不能进行数据插入，不会返回 Table
//        tableEnv.createTemporaryView("stock", stockStream);

        // TODO 创建永久表。表在显示删除之前一直可以查询，可以进行数据插入  table.executeInsert
        final Table table = tableEnv.fromDataStream(stockStream);
        table.executeInsert("stock");

//        String sql = "select id,stockName,avg(price) as priceavg from stock where stockName='UDFStock' group by id,stockName";
        sql = "select id,stockName,avg(price) as priceavg from stock where stockName='UDFStock' group by id,stockName";
        final Table sqlTable = tableEnv.sqlQuery(sql);
        //转换成流
        final DataStream<Tuple2<Boolean, Tuple3<String, String, Double>>> sqlTableDataStream = tableEnv.toRetractStream(sqlTable, TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {
        }));
        sqlTableDataStream.print("sql");

        env.execute("FileConnectorDemo");
    }
}
