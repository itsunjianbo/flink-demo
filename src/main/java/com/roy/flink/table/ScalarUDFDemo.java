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
import org.apache.flink.table.functions.ScalarFunction;

import java.net.URL;

/**
 * Table3 UDF方法  标量函数 ScalarFunction
 * 标量函数可以将0个或者多个标量值，映射成一个新的标量值。例如常见的获取
 * 当前时间、字符串转大写、加减法、多个字符串拼接，都是属于标量函数。
 * @author roy
 * @date 2021/9/12
 * @desc
 */
public class ScalarUDFDemo {
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
        tableEnv.createTemporaryView("stock", stockTable);

        // 注册UDF函数
        tableEnv.createTemporaryFunction("myConcate", new MyConcate());

        // 执行sql
        String sql = "select id,stockName,myConcate(stockName,price) as stockinfo from stock where stockName='UDFStock'";
        final Table sqlTable = tableEnv.sqlQuery(sql);

        //转换成流
        final DataStream<Tuple2<Boolean, Tuple3<String, String, String>>> sqlTableDataStream =
                tableEnv.toRetractStream(sqlTable, TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {
                }));
        sqlTableDataStream.print("sql");

        env.execute("ScalarUDFDemo");
    }

    public static class MyConcate extends ScalarFunction {
        // 实现ScalarFunction 没有提示，必须重新的方法
        // 必须实现一个public的eval函数，参数不能是Object，返回类型和参数类型不确定，根据实际情况定。
        // 这是目前版本完全没有道理的实现方式。
        public String eval(String a, Double b) {
            return a.toString() + "_" + b.toString();
        }
    }


}