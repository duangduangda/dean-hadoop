package org.dean.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.dean.toolkit.JDBCUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @description: 自定义输出端:mysql
 * @author: dean
 * @create: 2019/06/20 10:42
 */
public class MysqlSink extends RichSinkFunction<String> {

    private PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = JDBCUtils.getConnection("jdbc:mysql://localhost:3306/test","root","");
        String sql = "insert into word_count values(?,?);";
        ps = JDBCUtils.getPreparedStatement(connection,sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        JDBCUtils.close(connection,ps);
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(String value, Context context) throws Exception {
        //组装数据，执行插入操作
        if (null != value) {
            ps.setString(1, value);
            ps.setInt(2, 1);
        }
        ps.executeUpdate();
    }


}

