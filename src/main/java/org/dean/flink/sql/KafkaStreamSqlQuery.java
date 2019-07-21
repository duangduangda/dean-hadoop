package org.dean.flink.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.dean.flink.util.KafkaUtils;

/**
 * @description: 消费kafka数据
 * @author: dean
 * @create: 2019/07/21 18:13
 */
public class KafkaStreamSqlQuery {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.getTableEnvironment(environment);
        FlinkKafkaConsumer010 consumer = new FlinkKafkaConsumer010("test", new SimpleStringSchema(), KafkaUtils.config());
        consumer.setStartFromEarliest();
        DataStreamSource<String>book = environment.addSource(consumer);
        book.map(new MapFunction<String, Object>() {
            @Override
            public String map(String book) throws Exception {
                return book;
            }
        });
        tableEnvironment.registerDataStream("book", book,"book");
        String sql = "SELECT book,count(book) as counter FROM book GROUP BY book ";
        Table table = tableEnvironment.sqlQuery(sql);
        tableEnvironment.toRetractStream(table, Row.class).print();

        environment.execute();
    }
}
