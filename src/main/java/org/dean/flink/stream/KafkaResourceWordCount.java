package org.dean.flink.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description: Kafka数据流词频统计
 * @author: dean
 * @create: 2019/05/25 14:55
 */
public class KafkaResourceWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> kafkaDataStream = environment.addSource(new FlinkKafkaConsumer<String>("test",new SimpleStringSchema(),getKafkaProperties()));
        kafkaDataStream.print();
        environment.execute("Java word count with kafka data stream.");
    }

    /**
     * 设置kafka properties
     * @return
     */
    private static Properties getKafkaProperties(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        return properties;
    }
}
