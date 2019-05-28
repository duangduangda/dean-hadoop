package org.dean.flink.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description: 流转化
 * @author: dean
 * @create: 2019/05/25 21:42
 */
public class StreamTransformation {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        mapTrans(environment);
        filterTrans(environment);
        unionTrans(environment);
        environment.execute();
    }

    /**
     * transformation:union
     * @param environment
     */
    private static void unionTrans(StreamExecutionEnvironment environment) {
        DataStream<Integer> dataStream1 = environment.fromElements(1,3,4,5,6);
        DataStream<Integer> dataStream2 = environment.fromElements(2,3,1,23,4);
        DataStream<Integer> dataStream3 = dataStream1.union(dataStream2);
        dataStream3.print();
        DataStream<Integer> allDataStreams = dataStream1.union(dataStream2,dataStream3);
        allDataStreams.print();
    }

    /**
     * transformation:filter
     * @param environment
     */
    private static void filterTrans(StreamExecutionEnvironment environment) {
        DataStream<Integer> dataStream = environment.fromElements(1,2,3,4,4).filter(new FilterFunction<Integer>() {
            public boolean filter(Integer element) throws Exception {
                if (element % 2 == 0){
                    return true;
                }
                return false;
            }
        });
        dataStream.print();

    }

    /**
     * transformation:map
     * @param environment
     */
    private static void mapTrans(StreamExecutionEnvironment environment) {
        DataStream<Integer> dataStream = environment.fromElements(1,2,3,4,5).map(new MapFunction<Integer, Integer>() {
            public Integer map(Integer value) throws Exception {
                return value * value;
            }
        });
        dataStream.print();
    }
}
