package com.roy.flink.sink;

import com.roy.flink.beans.Stock;
import com.roy.flink.source.UDFSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 自定义 输出
 *
 * @author roy
 * @date 2021/9/7
 * @desc
 */
public class UDFJDBCSinkDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStreamSource<Stock> source = env.addSource(new UDFSource.MyOrderSource());

        source.addSink(new MyJDBCSink());

        env.execute("UDFJDBCSinkDemo");
    }


    // RichSinkFunction 自定义的输出Sink接收器
    public static class MyJDBCSink extends RichSinkFunction<Stock> {
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        // 初始方法，此处作用为初始语句
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://sunm.cloud:3306/sun_test", "sunm", "sunm123456");
            insertStmt = connection.prepareStatement("insert into flink_stock (id, price,stockname) values (?, ?, ?)");
            updateStmt = connection.prepareStatement("update flink_stock set price = ?,stockname = ? where id = ?");
        }

        // 结束执行
        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }

        // 进行中执行
        @Override
        public void invoke(Stock value, Context context) throws Exception {
            System.out.println("更新记录 ： " + value);
            updateStmt.setDouble(1, value.getPrice());
            updateStmt.setString(2, value.getStockName());
            updateStmt.setString(3, value.getId());
            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getPrice());
                insertStmt.setString(3, value.getStockName());
                insertStmt.execute();
            }
        }
    }

}
