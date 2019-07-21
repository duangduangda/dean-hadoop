package org.dean.flink.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @description: 数据源来自于socket，通过sql api查询
 * @author: dean
 * @create: 2019/07/18 10:31
 */
public class SocketSqlQuery {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.getTableEnvironment(environment);
        // 定义数据源
        DataStream<String> socketDataStream = environment.socketTextStream("localhost",9999);
        // 注册数据源
        tableEnvironment.registerDataStream("cities", socketDataStream,"city");
        // 执行查询
        String sql = "SELECT city,count(city) AS counter FROM cities GROuP BY city";
        Table table = tableEnvironment.sqlQuery(sql);
        tableEnvironment.toRetractStream(table, Row.class).print();
        environment.execute("Socket sql query");
    }
}
