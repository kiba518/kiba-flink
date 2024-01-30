package org.kiba.utils;

import cn.hutool.core.util.ObjectUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DatabaseConnectionUtils {
    private static Connection connectionWriter = null;

    public static Connection getWriterConn() throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
        if (ObjectUtil.equals( connectionWriter,null)) {
            String url = "jdbc:mysql://10.1.0.145:3306/testdb1?rewriteBatchedStatements=true";
            String username = "remote_user";
            String password = "123456";
            connectionWriter = DriverManager.getConnection(url, username, password);
        }
        connectionWriter.setAutoCommit(false);
        return connectionWriter;
    }

    private static Connection connectionReader = null;

    public static Connection getReaderConn() throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
        if (ObjectUtil.equals(connectionReader,null)) {
            String url = "jdbc:mysql://10.1.0.145:3306/testdb1?rewriteBatchedStatements=true";
            String username = "remote_user";
            String password = "123456";
            connectionReader = DriverManager.getConnection(url, username, password);
        }
        connectionWriter.setAutoCommit(false);
        return connectionReader;
    }

    private static PreparedStatement PreparedStatementWriter = null;

    public static PreparedStatement getPreparedStatementWriter() throws SQLException, ClassNotFoundException {

        if (ObjectUtil.equals(PreparedStatementWriter,null)) {
            PreparedStatementWriter = getWriterConn().prepareStatement("insert into testdb1.test_loginfo_result values (?,?,?)");
        }
        return PreparedStatementWriter;
    }

    public volatile static boolean execution = false;
}