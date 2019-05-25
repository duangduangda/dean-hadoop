package org.dean.flink.stream;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import java.util.Properties;

/**
 * @description: Wikiped
 * @author: dean
 * @create: 2019/05/21 18:12
 */
public class WikipediaAnalysis {
    public static void main(String[] args) throws Exception {
        // 获取环境信息
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 添加信息源
        DataStream<WikipediaEditEvent> edits = environment.addSource(new WikipediaEditsSource());
        // 根据事件中的用户名为key来区分数据流
        KeyedStream<WikipediaEditEvent, String> keyedStream = edits.keyBy(new KeySelector<WikipediaEditEvent, String>() {
            public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
                return wikipediaEditEvent.getUser();
            }
        });
        //
        DataStream<Tuple2<String, Long>> result = keyedStream
                .timeWindow((Time.seconds(5)))
                .fold(new Tuple2<String, Long>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) throws Exception {
                        acc.f0 = event.getUser();
                        acc.f1 = Long.valueOf(event.getByteDiff());
                        return acc;
                    }
                });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        result.print();
        environment.execute("Run wikipedia analysist");
    }
}
