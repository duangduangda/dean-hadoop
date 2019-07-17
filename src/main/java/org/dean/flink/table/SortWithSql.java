package org.dean.flink.table;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.dean.flink.domain.Community;

/**
 * @description: 使用sql进行排序
 * @author: dean
 * @create: 2019/07/02 19:22
 */
public class SortWithSql {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.getTableEnvironment(environment);
        DataSet<Community> communityDataSet = environment.readCsvFile("/Users/dean/communities.csv")
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .ignoreFirstLine()
                .pojoType(Community.class, "groupId", "groupName", "creatorAccountId", "createTime", "imageUrl", "deleteTime");
        tableEnvironment.registerDataSet("t_community", communityDataSet);
        Table table = tableEnvironment.scan("t_community")
                .groupBy("creatorAccountId")
                .select("creatorAccountId,creatorAccountId.count as counter")
                .filter("counter > 2")
                .orderBy("counter.desc");
        // 使用完整的sql
//        Table table = tableEnvironment.sqlQuery("SELECT creatorAccountId,COUNT(1) as counter FROM t_community GROUP BY creatorAccountId ORDER BY counter ASC").filter("counter > 2");
        tableEnvironment.toDataSet(table, Row.class).print();
    }
}
