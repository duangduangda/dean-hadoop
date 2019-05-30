package org.dean.flink.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description: 数据存储至csv
 * @author: dean
 * @create: 2019/05/28 19:26
 */
public class Sink2Csv {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment
                .fromElements(new Tuple2<>("eric",1), new Tuple2<>("duang",2),new Tuple2<>("hello",3),new Tuple2<>("flink",4),new Tuple2<>("scala",5))
                .filter(value -> value.f1 % 2 == 0).forceNonParallel().writeAsCsv("/Users/yaohua.dong/hello");
        environment.execute("Sink to csv file");
    }
}
