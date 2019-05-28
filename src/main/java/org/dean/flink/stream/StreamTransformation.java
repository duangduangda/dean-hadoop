package org.dean.flink.stream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 */
public class StreamTransformation {
    public static void main(String[] args) throws Exception {
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
        DataStream<Integer> dataStream1 = environment.fromElements(1, 3, 4, 5, 6);
        DataStream<Integer> dataStream2 = environment.fromElements(2, 3, 1, 23, 4);
        DataStream<Integer> dataStream3 = dataStream1.union(dataStream2);
        dataStream3.print();
        DataStream<Integer> allDataStreams = dataStream1.union(dataStream2, dataStream3);
        allDataStreams.print();
    }

    /**
     * transformation:filter
     * @param environment
     */
    private static void filterTrans(StreamExecutionEnvironment environment) {
        environment.fromElements(1, 2, 3, 4, 4)
                .filter(element -> element % 2 == 0).print();
    }

    /**
     * transformation:map
     * @param environment
     */
    private static void mapTrans(StreamExecutionEnvironment environment) {
        environment
                .fromElements(1, 2, 3, 4, 5)
                .map(value -> value * value)
                .print();
    }
}
