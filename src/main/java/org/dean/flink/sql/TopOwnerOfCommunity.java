package org.dean.flink.sql;

import com.google.common.base.Splitter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.dean.flink.domain.Community;

import java.util.List;

/**
 * @description: 查询社群最多的用户
 * @author: dean
 * @create: 2019/07/21 16:21
 */
public class TopOwnerOfCommunity {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境变量
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.getTableEnvironment(environment);

        // 2. 读取数据源
        DataSource<String> textFile = environment.readTextFile("/Users/dean/communities.csv");
        DataSet<Community> communityDataSet = textFile.map(new MapFunction<String, Community>() {
            @Override
            public Community map(String value) throws Exception {
                if (StringUtils.isNotBlank(value)){
                    List<String> stringList = Splitter.on(",")
                            .splitToList(value);
                    Community community = new Community();
                    community.setGroupId(stringList.get(0));
                    community.setGroupName(stringList.get(1));
                    community.setCreatorAccountId(stringList.get(2));
                    community.setCreateTime(stringList.get(3));
                    community.setImageUrl(stringList.get(4));
                    community.setDeleteTime(stringList.get(5));
                    return community;
                }
                return null;
            }
        });

        // 3. 注册数据源
        tableEnvironment.registerDataSet("community", communityDataSet);
        // 4. 执行查询并输出结果
        String sql = "SELECT creatorAccountId as id,count(groupId) AS counter FROM community " +
                "GROUP BY creatorAccountId ORDER BY counter DESC limit 10";
        Table result = tableEnvironment.sqlQuery(sql);
        tableEnvironment.toDataSet(result,TopN.class).print();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class TopN {
        private String id;
        private Long counter;
    }
}
