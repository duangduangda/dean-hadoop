package org.dean.flink.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.guava18.com.google.common.base.Charsets;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * @description: 将数据输出到csv文件
 * @author: dean
 * @create: 2019/07/22 14:24
 */
public class CsvSinkSqlUpdate {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.getTableEnvironment(environment);

        // 2. 设置数据源

//        DataStreamSource<Tuple2<String, Integer>> source = environment
//                .fromElements(new Tuple2<>("eric", 1), new Tuple2<>("duang", 2), new Tuple2<>("hello", 3), new Tuple2<>("flink", 4), new Tuple2<>("scala", 5));
        DataStream<String> source = environment
                .readTextFile("/communities.csv");
        tableEnvironment.registerDataStream("community",source, "groupId,groupName,creatorAccountId,createTime,imageUrl,deleteTime");
//        String sql = "SELECT creatorAccountId,COUNT(creatorAccountId) AS counter FROM community " +
//                "GROUP BY creatorAccountId ORDER BY counter DESC limit 10";
//        String sql = "SELECT * FROM community";
//        Table query = tableEnvironment.sqlQuery(sql);
//        tableEnvironment.toRetractStream(query, Row.class).print();

        // 3. 设置接收端
        TableSink sink = new CsvTableSink("/csv_sink",",", 1, FileSystem.WriteMode.OVERWRITE);
        String[] fieldNames = {"creator","amount"};
        TypeInformation[] fieldTypes = {Types.STRING, Types.INT};
        tableEnvironment.registerTableSink("community_summary",fieldNames,fieldTypes, sink);

        // 4. 执行sql，并输出到接收端
        String update = "INSERT INTO community_summary " +
                "SELECT creatorAccountId,COUNT(creatorAccountId) AS counter FROM community GROUP BY creatorAccountId ORDER BY counter DESC LIMIT 10 ";
        tableEnvironment.sqlUpdate(update);
        environment.execute();
    }
}
