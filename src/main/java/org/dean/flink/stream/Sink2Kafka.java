package org.dean.flink.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @description: 输出到kafka
 * @author: dean
 * @create: 2019/06/19 17:32
 */
public class Sink2Kafka {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = environment.fromElements("中国话","美籍华人","21321321321");
        input.addSink(new FlinkKafkaProducer011<>("consumer_test",new SimpleStringSchema(),kafkaConig()));
        environment.execute("Sink to kafka");

    }

    private static Properties kafkaConig() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");//broker地址
        properties.setProperty("zookeeper.connect", "localhost:2181");//zookeeper配置
        properties.setProperty("group.id", "flink_group");
        return properties;
    }
}
