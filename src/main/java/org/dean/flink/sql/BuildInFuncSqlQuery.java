package org.dean.flink.sql;

import com.google.common.base.Splitter;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.dean.flink.domain.WC;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @description: 使用内置函数进行词频统计
 * @author: dean
 * @create: 2019/07/21 09:06
 */
class BuildInFuncSqlQuery {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.getTableEnvironment(environment);
        String expression = "hello flink hello scala hello kafka";
        List<WC> words = Splitter.on(" ")
                .omitEmptyStrings()
                .trimResults()
                .splitToList(expression)
                .stream()
                .map(word -> transfer2POJO(word))
                .collect(Collectors.toList());
        DataSet<WC> dataSet = environment.fromCollection(words);
        tableEnvironment.registerDataSet("word_freq", dataSet);
        // 默认的sql
//        String sql = "SELECT word,sum(counter) as counter FROM word_freq GROUP BY word";
        // 只查询hello的记录
//        String sql = "SELECT word,sum(counter) as counter FROM word_freq WHERE word = 'hello' GROUP BY word";
        String sql = "SELECT word,sum(counter) as counter FROM word_freq GROUP BY word";
        Table table = tableEnvironment.sqlQuery(sql);
        tableEnvironment.toDataSet(table,WC.class).filter(wc -> wc.counter > 2).print();
    }

    private static WC transfer2POJO(String word) {
        WC wordCounter = new WC();
        wordCounter.setWord(word);
        wordCounter.setCounter(1);
        return wordCounter;
    }
}
