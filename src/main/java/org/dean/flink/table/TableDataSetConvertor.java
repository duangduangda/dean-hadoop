package org.dean.flink.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.dean.flink.domain.WC;

/**
 * @description:
 * @author: dean
 * @create: 2019/06/14 22:45
 */
public class TableDataSetConvertor {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.getTableEnvironment(executionEnvironment);

        DataSet<WC> input = executionEnvironment.fromElements(
                new WC("Hello", 1),
                new WC("zhisheng", 1),
                new WC("Hello", 1));
        Table table = tEnv.fromDataSet(input,"word,counter");
        Table filtered = table
                .groupBy("word")
                .select("word, counter.sum as counter")
                .filter("counter >= 1");
        // 如果sql查询的结果集最终转化为WC，那么select里的字段别名必须和WC中的一致
        DataSet<WC> result = tEnv.toDataSet(filtered, WC.class);
        result.print();
    }
}
