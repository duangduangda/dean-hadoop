package org.dean.flink.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.dean.flink.util.KafkaUtils;

/**
 * @description: 输出到kafka
 * @author: dean
 * @create: 2019/06/19 17:32
 */
public class ElementsSink2Kafka {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = environment.fromElements("中国话","美籍华人","21321321321");
        input.addSink(new FlinkKafkaProducer011<String>("test",new SimpleStringSchema(), KafkaUtils.config()));
        environment.execute("Sink to kafka");

    }
}
