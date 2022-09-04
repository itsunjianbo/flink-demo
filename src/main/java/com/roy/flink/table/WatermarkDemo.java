package com.roy.flink.table;

import com.roy.flink.streaming.FileRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * @author roy
 * @date 2021/9/13
 * @desc  DDL方式定义Table的EventTime。 失败，timestamp的类型不知道怎么转换。
 */
public class WatermarkDemo {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-DD HH:mm:ss");
    public static void main(String[] args) throws Exception {
        //1、读取数据
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final URL resource = FileRead.class.getResource("/stock.txt");
        final String filePath = resource.getFile();
        final DataStreamSource<String> dataStream = env.readFile(new TextInputFormat(new Path(filePath)), filePath);

        final SingleOutputStreamOperator<Tuple4<String,Double,String,Timestamp>> stockStream = dataStream
                .map(new MapFunction<String, Tuple4<String,Double,String,Timestamp>>() {
                    @Override
                    public Tuple4<String, Double, String, Timestamp> map(String value) throws Exception {
                        final String[] split = value.split(",");
                        long timeLong = Long.parseLong(split[3]);
                        timeLong = timeLong-timeLong%1000;
                        return new Tuple4<>(split[0], Double.parseDouble(split[1]), split[2], new Timestamp(timeLong));
                    }
                });
        //2、创建StreamTableEnvironment catalog -> database -> tablename
        final EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database").build();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);

        String sql = "create table stock(" +
                "            id varchar," +
                "            price double," +
                "            stockName varchar," +
                "            eventtime timestamp(3)," +
                "            WATERMARK FOR eventtime AS eventtime - INTERVAL '5' SECOND" +
                "          ) with (" +
                "            'connector.type' = 'filesystem'," +
                "            'format.type' = 'csv'," +
                "            'connector.path' = 'D://flinktable'" +
                "          )";
        tableEnv.executeSql(sql);

        final Table table = tableEnv.fromDataStream(stockStream);
        table.executeInsert("stock");

//        String querySql = "select TUMBLE_START(`eventtime`,INTERVAL '10' MINUTE),COUNT(DISTINCT id) from stock group by TUMBLE(`eventtime`, INTERVAL '10' MINUTE)";
//        final Table sqlTable = tableEnv.sqlQuery(querySql);
//        //转换成流
//        final DataStream<Tuple2<Boolean, Tuple2<Timestamp, Long>>> sqlTableDataStream =
//                tableEnv.toRetractStream(sqlTable, TypeInformation.of(new TypeHint<Tuple2<Timestamp, Long>>() {
//        }));
//        sqlTableDataStream.print("querySql");

        String querySql = "select id,eventtime from stock";
        final Table sqlTable = tableEnv.sqlQuery(querySql);
        //转换成流
        final DataStream<Tuple2<Boolean, Tuple2<String, Timestamp>>> sqlTableDataStream =
                tableEnv.toRetractStream(sqlTable, TypeInformation.of(new TypeHint<Tuple2<String, Timestamp>>() {
                }));
        sqlTableDataStream.print("querySql");

        env.execute("FileConnectorDemo");
    }
}
