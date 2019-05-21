package org.dean.flink.stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.StringTokenizer;

/**
 * @description: stream word counter
 * @author: dean
 * @create: 2019/03/16 20:38
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //在命令行执行nc -l 9999 开启交互监听获取数据
        final DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        //计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new WordSplitter())
                .keyBy(0)
                .sum(1);

        sum.print();
        env.execute("Java WordCount from SocketTextStream Example");
    }

    /**
     * mr
     */
    private static final class WordSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            StringTokenizer stringTokenizer = new StringTokenizer(value,"\\W+");
            while (stringTokenizer.hasMoreTokens()) {
                String token = stringTokenizer.nextToken();
                if (StringUtils.isNotBlank(token)) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
