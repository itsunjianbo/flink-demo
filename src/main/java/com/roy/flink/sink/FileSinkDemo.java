package com.roy.flink.sink;

import com.roy.flink.streaming.FileRead;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import java.net.URL;

/**
 * @author roy
 * @date 2021/9/7
 * @desc
 */
public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        // 设置并行度，设置为1就是一个输出文件
//        env.setParallelism(1);

        final URL resource = FileRead.class.getResource("/test.txt");
        final String filePath = resource.getFile();
        final DataStreamSource<String> stream = env.readTextFile(filePath);

        // 输出文件配置
        OutputFileConfig outputFileConfig = OutputFileConfig
                .builder()
                // 文件前缀
                .withPartPrefix("prefix")
                .withPartSuffix(".txt")
                .build();

        // 第一种构建接收器
        // 流式文件接收器
        final StreamingFileSink<String> streamingfileSink = StreamingFileSink
                .forRowFormat(new Path("D:/ft"), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(outputFileConfig)
                .build();

        // 为数据流 添加 接收器
        stream.addSink(streamingfileSink);

        // 第二种构建接收器
//        final FileSink<String> fileSink = FileSink
//                .forRowFormat(new Path("D:/ft"), new SimpleStringEncoder<String>("UTF-8"))
//                .withOutputFileConfig(outputFileConfig)
//                .build();
//        stream.sinkTo(fileSink);

        env.execute("FileSink");
    }
}
