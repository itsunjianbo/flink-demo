package com.roy.flink.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        //读取本地文件
        final DataSource<String> stringDataSource = environment.readTextFile("E:\\a-work\\flink\\wc.txt");
        //进行wordcount操作
        final DataSet<Tuple2<String, Integer>> wordCount = stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                final String[] words = value.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })//通过flatmap将内容读取成(word,1)这样的元祖
                .setParallelism(2) //设置flatmap操作的并行度2
                .groupBy(0)//按第1个元素分组
                .sum(1)//按第2个元素求和
                .setParallelism(1);//设置flatmap操作的并行度1
        //输出到标准窗口
        wordCount.print();
    }
}
