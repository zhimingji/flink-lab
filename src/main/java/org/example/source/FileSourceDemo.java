package org.example.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo  从文件读：新source框架
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("src/main/java/org/example/input/word.txt"))
                .build();

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(),"fileSource").print();

        env.execute();
    }
}

/**
 * 新Source写法：
 *  env.formSource(Source的实现类，Watermark,名字)
 * */
