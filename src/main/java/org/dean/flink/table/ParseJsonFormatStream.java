package org.dean.flink.table;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.dean.flink.domain.Word;
import org.dean.flink.util.KafkaUtils;
import org.dean.toolkit.JSONUtils;

/**
 * @description: 解析json格式数据流(集合-kafka生产端-kafka消费端-stream处理)
 * @author: dean
 * @create: 2019/06/21 11:30
 */
public class ParseJsonFormatStream {
    private static final String KAFKA_CONSUME_TOPIC = "test";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = environment.fromElements(
                JSONUtils.toJsonStr(new Word("中国")),
                JSONUtils.toJsonStr(new Word("美国")),
                JSONUtils.toJsonStr(new Word("中国")),
                JSONUtils.toJsonStr(new Word("美国")),
                JSONUtils.toJsonStr(new Word("英国")),
                JSONUtils.toJsonStr(new Word("澳大利亚")));
        input.addSink(new FlinkKafkaProducer011<>(KAFKA_CONSUME_TOPIC, new SimpleStringSchema(), KafkaUtils.config()));

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.getTableEnvironment(environment);
        Kafka kafka = new Kafka().version("0.11")
                .topic(KAFKA_CONSUME_TOPIC)
                .properties(KafkaUtils.config())
                .startFromLatest()
                .sinkPartitionerFixed();
        Schema schema = new Schema().field("word",Types.STRING);
        tableEnvironment.connect(kafka)
                .withFormat(new Json()
                        .failOnMissingField(false)
                        .schema(Types.ROW_NAMED(new String[]{"word"},new TypeInformation[]{Types.STRING})))
                .withSchema(schema)
                .inAppendMode().registerTableSource("t_json");

        Table table = tableEnvironment.scan("t_json")
                .groupBy("word")
                .select("word,word.count as counter");

        tableEnvironment.toRetractStream(table, Row.class).print();
        environment.execute("Parse json format and sink to mysql");
    }
}
