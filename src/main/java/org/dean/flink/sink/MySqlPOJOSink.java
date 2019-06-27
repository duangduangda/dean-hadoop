package org.dean.flink.sink;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.dean.flink.domain.WC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * @description: 使用对象映射对数据进行转化并输出到mysql
 * @author: dean
 * @create: 2019/06/20 19:07
 */
public class MySqlPOJOSink extends RichSinkFunction<List<WC>> {

    private static final Logger logger = LoggerFactory.getLogger(MySqlPOJOSink.class);

    private static final GsonBuilder gsonBuilder = new GsonBuilder();
    private static final Gson gson = gsonBuilder.create();

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
        connection = getConnection();
        String sql = "insert into word_count(word,count) values(?,?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param values
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<WC> values, Context context) throws Exception {
        //组装数据，执行单行插入操作
//        for (WC wc:values){
//            ps.setString(1,wc.getWord());
//            ps.setInt(2, wc.getCount());
//            ps.executeUpdate();
//            logger.info("[DB] insert into word_count.WC:{}", gson.toJson(wc));
//        }
        //使用批量插入
        for (WC wc:values){
            ps.setString(1,wc.getWord());
            ps.setInt(2,wc.getCounter());
            ps.addBatch();
        }

        int[]count = ps.executeBatch();
        logger.info("[DB] batch insert {} lines",count.length);
    }

    /**
     * 获取数据库连接
     *
     * @return
     */
    private static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return connection;
    }
}


