package org.dean.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.dean.flink.domain.WC;

/**
 * @description: 使用sql接口查询，并输出去至csv文件
 * @author: dean
 * @create: 2019/06/24 14:06
 */
public class TableSqlQuery{
    public static void main(String[] args) throws Exception{
        // 获取stream运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取table运行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.getTableEnvironment(environment);
        // 初始化数据源
        DataStream<WC> dataStream = environment.fromElements(
                WC.build().setWord("Hello").setCounter(1),
                WC.build().setWord("Scala").setCounter(2),
                WC.build().setWord("Flink").setCounter(3));
        // 注册数据源table，设置filed的时候不能使用sql语句的关键字，否则执行sql失败
        tableEnvironment.registerDataStream("word_table",dataStream,"word as content,counter");
        // 执行query,获取结果集table
        Table table = tableEnvironment.sqlQuery("select * from word_table where content = 'Hello'");
        tableEnvironment.registerTableSink("csv_table",
                new CsvTableSink("/Users/dean/csv",
                        ",",
                        1, FileSystem.WriteMode.OVERWRITE).configure(new String[]{"content","counter"},new TypeInformation[]{Types.STRING,Types.INT}));
        table.insertInto("csv_table");
        // 输出到控制台。使用TypeExtrator.createTypeInfo，查询的字段必须和pojo中的命名一致
//        tableEnvironment.toAppendStream(table, TypeExtractor.createTypeInfo(WC.class) ).print();
        environment.execute();
    }
}
