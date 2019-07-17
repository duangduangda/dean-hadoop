package org.dean.flink.transformation;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;


public class MultiplyTwoNumberTest {

    @Test
    public void testMap() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        MyCollectSink.values.clear();

        environment.fromElements(1,23,4,5)
                .map(new MultiplyTwoNumber())
                .addSink(new MyCollectSink());

        environment.execute();

        assertEquals(Lists.newArrayList(2,46,8,10), MyCollectSink.values);
    }

    private static class MyCollectSink implements SinkFunction<Integer>{
        public static final List<Integer> values = Lists.newArrayList();

        @Override
        public synchronized void invoke(Integer value, SinkFunction.Context context) throws Exception{
            values.add(value);
        }

    }
}