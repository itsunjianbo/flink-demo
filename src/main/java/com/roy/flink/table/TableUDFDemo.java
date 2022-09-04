package com.roy.flink.table;

import com.roy.flink.beans.Stock;
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
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.net.URL;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Table4 UDF方法  表函数 TableFunction
 * 表函数同样以0个或者多个标量作为输入，但是他可以返回任意数量的行作为输
 * 出，而不是单个值。
 * @author roy
 * @date 2021/9/12
 * @desc
 */
public class TableUDFDemo {
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
        tableEnv.createTemporaryView("stock",stockTable);

        //注册TableFunction
        tableEnv.createTemporaryFunction("splitId",new SplitFunction());

        // TODO table方式调用
        final Table tableRes
                = tableEnv.from("stock")
                // 调用了 SplitFunction，生产了2个字段，可以理解为生产了1个新的表，所以叫表函数
                .joinLateral(call(SplitFunction.class, $("id")))
                // word 和 length 字段是 SplitFunction() 方法的结果
                .select($("id"), $("word"), $("length"), $("price"));
        tableEnv.toAppendStream(tableRes,TypeInformation.of(new TypeHint<Tuple4<String, String, Integer , Double>>(){})).print("tableres");

        // TODO sql中调用,这2个sql的作用是一样的
//        String sql = "select id,word,length from stock LEFT JOIN LATERAL TABLE(splitId(id))";
        String sql = "select id,word,length,price from stock ,LATERAL TABLE(splitId(id))";
        final Table sqlTable = tableEnv.sqlQuery(sql);
        //转换成流
        final DataStream<Tuple2<Boolean, Tuple4<String, String, Integer,Double>>> sqlTableDataStream =
                tableEnv.toRetractStream(sqlTable, TypeInformation.of(new TypeHint<Tuple4<String, String, Integer, Double>>() {}));
        sqlTableDataStream.print("sql");



        env.execute("TableUDFDemo");
    }

    // 申明收集字段，2个字段，第1个字段为string类型，第2个字段为int
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {

        public void eval(String str){
            for (String s : str.split("_")) {
                // use collect(...) to emit a row
                collect(Row.of(s, s.length()));
            }
        }
    }

    /*
    将一条stock_578,22.141256900167285,UDFStock,1631002965778数据拆分成两条数据
    stock_578,stock,5,22.141256900167285,UDFStock,1631002965778
    stock_578,578,3,22.141256900167285,UDFStock,1631002965778
     */
}
