package org.dean.flink.stream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.dean.flink.sink.MysqlSink;


/**
 * @description: 集合元素输出到mysql中
 * @author: dean
 * @create: 2019/06/20 14:58
 */
public class ElementsSink2Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = environment.fromElements("Hello","Flib","flink");
        dataStream.addSink(new MysqlSink());
        environment.execute("Stream data sink to mysql");
    }
}
