package org.dean.flink.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @description: jdbc工具类
 * @author: dean
 * @create: 2019/06/21 15:12
 */
public class JDBCUtils {
    /**
     * 获取数据库连接
     * @return
     */
    public static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }

    /**
     * 关闭数据库资源
     * @param connection
     * @param ps
     */
    public static void close(Connection connection, PreparedStatement ps) {
        try {
            //关闭连接和释放资源
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 获取preparedstatement
     * @param connection
     * @param sql
     * @return
     */
    public static PreparedStatement getPreparedStatement(Connection connection, String sql) {
        try {
            return connection.prepareStatement(sql);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
