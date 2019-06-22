package org.dean.flink.stream;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.dean.flink.domain.WC;
import org.dean.flink.sink.MySqlPOJOSink;
import org.dean.flink.util.KafkaUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @description: 使用pojo对kafka消息进行转化，同时转存到mysql
 * @author: dean
 * @create: 2019/06/22 19:08
 */
public class KafkaPOJOSink2Mysql {
    private static final GsonBuilder gsonBuilder = new GsonBuilder();
    private static final Gson gson = gsonBuilder.create();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = environment.fromElements(
                gson.toJson(new WC("CHINA", 2)),
                gson.toJson(new WC("USA", 2)),
                gson.toJson(new WC("UK", 1)));
        input.addSink(new FlinkKafkaProducer011<>("test", new SimpleStringSchema(), KafkaUtils.config()));

        SingleOutputStreamOperator<WC> words = environment.addSource(new FlinkKafkaConsumer011<>(
                "test",
                new SimpleStringSchema(),
                KafkaUtils.config())).setParallelism(1)
                .map(value -> gson.fromJson(value, WC.class)); //
        words.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<WC, List<WC>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<WC> values, Collector<List<WC>> out) throws Exception {
                ArrayList<WC> words = Lists.newArrayList(values);
                if (words.size() > 0) {
                    System.out.println("1 分钟内收集到 student 的数据条数是：" + words.size());
                    out.collect(words);
                }
            }
        }).addSink(new MySqlPOJOSink());
        environment.execute("Flink kafka connector run");
    }
}
