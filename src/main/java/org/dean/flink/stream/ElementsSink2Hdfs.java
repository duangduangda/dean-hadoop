package org.dean.flink.stream;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * @description:集合元素输出到Hdfs
 * @author dean
 * @since 2019-06-20
 */
public class ElementsSink2Hdfs {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<Integer,String> dataMap = Maps.newHashMap();
        dataMap.put(1,"Spark");
        dataMap.put(2,"Flink");
        dataMap.put(3,"Scala");
        dataMap.put(4,"Java");
        DataStream<Map<Integer,String>> mapDataStream = environment.fromElements(dataMap);
        // 输出到hdfs，并设置并发数，生成单一文件
        mapDataStream.writeAsText("hdfs://127.0.0.1:9001/user/dean/flink/example3/data", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        environment.execute("Element from collection sink to hdfs");
    }
}
