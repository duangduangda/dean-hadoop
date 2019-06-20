package org.dean.flink.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.dean.flink.domain.WC;
import org.dean.flink.sink.MysqlSink;
import org.dean.flink.util.KafkaUtils;

/**
 * @description: 接收kafka消息并保存在mysql中
 * @author: dean
 * @create: 2019/06/19 19:24
 */
public class KafkaSink2Mysql {

    private static final String KAFK_CONSUME_TOPIC = "test";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = environment.fromElements(
                new WC("Hello", 1).toString(),
                new WC("Scala", 1).toString(),
                new WC("Hello", 1).toString());
        input.addSink(new FlinkKafkaProducer011<>(KAFK_CONSUME_TOPIC, new SimpleStringSchema(), KafkaUtils.config()));
        SingleOutputStreamOperator<String> words = environment
                .addSource(new FlinkKafkaConsumer011<>(KAFK_CONSUME_TOPIC,
                        new SimpleStringSchema(),
                        KafkaUtils.config())).setParallelism(1);
        words.addSink(new MysqlSink());
        environment.execute("Kafka msg sink to mysql");
    }
}
