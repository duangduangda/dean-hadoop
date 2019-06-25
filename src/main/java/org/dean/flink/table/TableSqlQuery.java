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
                WC.build().setWord("Hello").setCount(1),
                WC.build().setWord("Scala").setCount(2),
                WC.build().setWord("Flink").setCount(3));
        // 注册数据源table
        tableEnvironment.registerDataStream("word_table",dataStream,"comment");
        // 执行query,获取结果集table
        Table table = tableEnvironment.sqlQuery("select * from word_table where count &lt; 1");
        tableEnvironment.registerTableSink("csv_table",
                new CsvTableSink("/Users/dean/csv",
                        ",",
                        1, FileSystem.WriteMode.OVERWRITE).configure(new String[]{"content"},new TypeInformation[]{Types.STRING}));
        table.insertInto("csv_table");
        environment.execute();
    }
}
