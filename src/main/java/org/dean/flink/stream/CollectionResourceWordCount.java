package org.dean.flink.stream;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.Arrays;
import java.util.List;

/**
 * @description: 来自集合的数据流词频统计
 * @author: dean
 * @create: 2019/05/24 23:10
 */
public class CollectionResourceWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Integer, Integer>> dataStream = getStreamFromElements(environment);
        dataStream.print();
        DataStream<Tuple2<String, Integer>> tuple2DataStream = getStreamFromCollection(environment);
        tuple2DataStream.print();
        covertList2DataStream(environment);
        environment.execute("Java word count fromm collection.");
    }

    /**
     * covert list to datastream
     * @param environment
     */
    private static void covertList2DataStream(StreamExecutionEnvironment environment) {
        List<String> arrayList = Lists.newArrayList("hello,flink","Hello,Hadoop");
        DataStream<String> dataStream = environment.fromCollection(arrayList);
        dataStream.print();
    }

    /**
     * get stream from elements
     * @param environment
     * @return
     */
    private static DataStream<Tuple2<Integer, Integer>> getStreamFromElements(StreamExecutionEnvironment environment) {
        return environment
                .fromElements(1, 2, 3, 4, 1, 1, 2, 3, 1, 1)
                .flatMap(new FlatMapFunction<Integer, Tuple2<Integer, Integer>>() {
                    public void flatMap(Integer element, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                        collector.collect(new Tuple2<Integer, Integer>(element, 1));
                    }
                })
                .keyBy(0)
                .sum(1);
    }

    /**
     * get stream from collection
     * @param environment
     * @return
     */
    private static DataStream<Tuple2<String, Integer>> getStreamFromCollection(StreamExecutionEnvironment environment) {
        String[] elements = new String[]{"HELLO", "hello", "flink"};
        return environment.fromCollection(Arrays.asList(elements)).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String element, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(new Tuple2<String, Integer>(element.toLowerCase(), 1));
            }
        }).keyBy(0).sum(1);
    }
}
