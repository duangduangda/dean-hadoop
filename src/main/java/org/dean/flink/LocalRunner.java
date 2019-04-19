package org.dean.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class LocalRunner {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createCollectionsEnvironment();
        // create a dataset of numbers
        DataSet<Integer> numbers = executionEnvironment.fromElements(1, 2, 3 ,4 , 5, 6,7,8,9,10);
        // square every  number
        DataSet<Integer> result = numbers.map(new MapFunction<Integer, Integer>() {
            public Integer map(Integer value) throws Exception {
                return value * value;
            }
        }).filter(new FilterFunction<Integer>() {
            public boolean filter(Integer value) throws Exception {
                return value % 2 != 0;
            }
        });
        result.print();
    }
}
