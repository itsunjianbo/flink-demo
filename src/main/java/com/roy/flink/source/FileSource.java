package com.roy.flink.source;

import com.roy.flink.streaming.FileRead;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;

/**
 * 文本数据源
 * TODO 这里我没有复现
 * @author roy
 * @date 2021/9/7
 */
public class FileSource {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        final URL resource = FileRead.class.getResource("/test.txt");
        final String filePath = resource.getFile();
        System.out.println(filePath);
//        final DataStreamSource<String> stream = env.readTextFile("D://test.txt");
//        final DataStreamSource<String> stream = env.readTextFile(filePath);
        final DataStreamSource<String> stream = env.readFile(new TextInputFormat(new Path(filePath)), filePath);

        stream.print();
        env.execute("FileSource");
    }
}
