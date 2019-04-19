package org.dean.flink.batch;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.StringTokenizer;

/**
 * @description: 词频统计
 * @author: dean
 * @create: 2019/03/10 00:39
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = environment.readTextFile(args[0]);
        DataSet<Tuple2<String,Integer>> counters = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] lines = value.split("\n");
                for (String token:lines){
                    StringTokenizer stringTokenizer = new StringTokenizer(token);
                    while (stringTokenizer.hasMoreTokens()){
                        collector.collect(new Tuple2<String, Integer>(stringTokenizer.nextToken(),1));
                    }
                }
            }
        }).groupBy(0).sum(1);

        counters.print();
        counters.writeAsCsv("/Users/dean");
//        counters.writeAsCsv("hdfs://localhost:9001/user/dean/flink/example1/output/flink","\n","\t");
        environment.execute();

    }
}
