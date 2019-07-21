package org.dean.flink.sql;

import com.google.common.base.Splitter;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.dean.flink.domain.WC;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @description: 用户自定义函数:
 *  1. 继承scalaFunction
 *  2. 覆盖写方法eval
 *  3. 注册函数
 *  4. 应用
 * @author: dean
 * @create: 2019/07/21 11:13
 */
public class UserDefineSqlQuery {
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
        tableEnvironment.registerFunction("stringToSite", new StringToSiteFunction(".com"));
        String sql = "SELECT stringToSite(word) AS word,sum(counter) AS counter FROM word_freq GROUP BY word";
        Table table = tableEnvironment.sqlQuery(sql);
        tableEnvironment.toDataSet(table,WC.class).print();
    }

    public static WC transfer2POJO(String word) {
        WC wordCounter = new WC();
        wordCounter.setWord(word);
        wordCounter.setCounter(1);
        return wordCounter;
    }

    /**
     * 自定义函数public，sql查询时使用，将输入的值，新增后缀
     *
     */
    public static class StringToSiteFunction extends ScalarFunction {
        private String address;
        public StringToSiteFunction(String address) {
            this.address = address;
        }

        public String eval(String value){
            StringBuilder stringBuilder = new StringBuilder(value);
            stringBuilder.append(address);
            return stringBuilder.toString();
        }
    }
}
