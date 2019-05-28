package org.dean.flink.stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.StringTokenizer;

/**
 * @description: 来自文件的数据流词频统计
 * @author: dean
 * @create: 2019/05/24 22:25
 */
public class TextResourceWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> dataStream = environment.readTextFile("/Users/dean/hadoop-2.8.5/script/input/file1.txt").flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) {
                StringTokenizer stringTokenizer = new StringTokenizer(value,"\n");
                while (stringTokenizer.hasMoreTokens()){
                    StringTokenizer tokenizer = new StringTokenizer(stringTokenizer.nextToken()," ");
                    while (tokenizer.hasMoreTokens()) {
                        String token = tokenizer.nextToken();
                        if (StringUtils.isNotBlank(token)) {
                            collector.collect(new Tuple2<String, Integer>(token, 1));
                        }
                    }
                }
            }
        }).keyBy(0).sum(1);
        dataStream.print();
        environment.execute("Java word count from text file");

    }
}
