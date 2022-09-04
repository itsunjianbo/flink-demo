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
import org.apache.flink.table.functions.AggregateFunction;

import java.net.URL;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Table5  UDF方法  聚合函数 AggregateFunction
 * 聚合函数可以把一个表中一列的数据，聚合成一个标量值。例如常用的max、
 * min、count这些都是聚合函数。定义聚合函数时，首先需要定义个累加器
 * Accumulator，用来保存聚合中间结果的数据结构，可以通过
 * createAccumulator()方法构建空累加器。然后通过accumulate()方法来对每一
 * 个输入行进行累加值更新。最后调用getValue()方法来计算并返回最终结果。
 * @author roy
 * @date 2021/9/13
 * @desc
 */
public class AggregateUDFDemo {

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
        //注册UDF函数
        tableEnv.createTemporaryFunction("myavg", new MyAvg());

        //Table方式调用函数。
        final Table tableRes = stockTable.groupBy($("id"), $("stockName"))
//                .aggregate(  $("MyAvg(price)").as("avgprice"))
                .aggregate(call(MyAvg.class,$("price")).as("avgprice"))
                .select($("id"), $("stockName"),$("avgprice"))
                .where($("stockName").isEqual("UDFStock"));
        //转换成流
        final DataStream<Tuple2<Boolean, Tuple3<String, String, Double>>> stableDataStream =
                tableEnv.toRetractStream(tableRes, TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {
                }));
        stableDataStream.print("stableDataStream");

//        //sql方式调动函数。
//        String sql = "select id,stockName,myavg(price) as avgprice from stock where stockName='UDFStock' group by id,stockName";
//        final Table sqlTable = tableEnv.sqlQuery(sql);
//        //转换成流
//        final DataStream<Tuple2<Boolean, Tuple3<String, String, Double>>> sqlTableDataStream =
//                tableEnv.toRetractStream(sqlTable, TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {
//                }));
//        sqlTableDataStream.print("sql");

        env.execute("ScalarUDFDemo");
    }

    // 聚合函数的实现
    // Double 为传入的类型，Tuple2<Double,Integer> 为累加器的类型
    public static class MyAvg extends AggregateFunction<Double,Tuple2<Double,Integer>> {

        //从累加器获取结果
        // 返回的是累加器的值处理的结果
        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0/accumulator.f1;
        }

        //创建累加器
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.00,0) ;
        }

        // 必须实现一个accumulate方法，来数据之后更新状态
        // 同样是一个目前版本没有道理的实现方式。
        public void accumulate( Tuple2<Double, Integer> accumulator, Double temp ){
            accumulator.f0 += temp;
            accumulator.f1 += 1;
        }
    }
}
