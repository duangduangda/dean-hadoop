package org.dean.flink.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.StringTokenizer;

/**
 * @description: stream word counter
 * @author: dean
 * @create: 2019/03/16 20:38
 */
public class WordCount {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 在命令行执行nc -l 9999 开启数据交互
        DataStream<String> dataStream = environment.socketTextStream("localhost",9999);
        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        StringTokenizer stringTokenizer = new StringTokenizer(value,"\t");
                        while (stringTokenizer.hasMoreTokens()){
                            collector.collect(new Tuple2<String, Integer>(stringTokenizer.nextToken(),1));
                        }
                        collector.collect(new Tuple2<String, Integer>(value,1));
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5)).sum(1);
        dataStream.print();
        environment.execute();
    }

}
