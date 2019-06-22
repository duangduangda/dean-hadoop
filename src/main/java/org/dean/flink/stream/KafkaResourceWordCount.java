package org.dean.flink.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.dean.flink.util.KafkaUtils;

/**
 * @description: 消费Kafka数据，进行词频统计
 * @author: dean
 * @create: 2019/05/25 14:55
 */
public class KafkaResourceWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> kafkaDataStream = environment.addSource(new FlinkKafkaConsumer011<>("test",new SimpleStringSchema(), KafkaUtils.config()));
        kafkaDataStream.print();
        environment.execute("Java word count with kafka data stream.");
    }
}
