package com.roy.flink.streaming;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import java.net.URL;

public class FileRead {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        final URL resource = FileRead.class.getResource("/test.txt");
        final String filePath = resource.getFile();
//        final DataStreamSource<String> stream = env.readTextFile(filePath);
        final DataStreamSource<String> stream = env.readFile(new TextInputFormat(new Path(filePath)), filePath);

        stream.print();
        OutputFileConfig outputFileConfig = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".txt")
                .build();
//        stream.writeAsText("D:/");
        final FileSink<String> fileSink = FileSink
                .forRowFormat(new Path("D:/ft"), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(outputFileConfig)
                .build();
        stream.sinkTo(fileSink);

        final StreamingFileSink<String> streamingfileSink = StreamingFileSink
                .forRowFormat(new Path("D:/ft"), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(outputFileConfig)
                .build();
        stream.addSink(streamingfileSink);
        env.execute();
    }
}
