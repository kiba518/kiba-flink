package org.kiba.utils;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.db.Db;
import cn.hutool.db.GlobalDbConfig;
import cn.hutool.log.level.Level;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DBConnectionUtils {


    static volatile DBConnectionUtils INSTANCE = null;

    public static DBConnectionUtils GetInstance() {
        if (ObjectUtil.equals(INSTANCE, null)) {
            INSTANCE = new DBConnectionUtils();
            return INSTANCE;
        } else {
            return INSTANCE;
        }
    }
    public static void close() throws SQLException {
        DBConnectionUtils.GetInstance().clear();
    }

    public static void init() throws SQLException, ClassNotFoundException {
        if (!ObjectUtil.equals(INSTANCE, null)) {
            INSTANCE.clear();
        }
        INSTANCE = null;
        GetInstance();
        Class.forName("com.mysql.cj.jdbc.Driver");//加载数据库驱动
        Connection connectionMDB = INSTANCE.getMiddleDBConn();
        GlobalDbConfig.setShowSql(true, true, true, Level.DEBUG);//开启hutool打印sql日志，但必须配置log4j2的debug级别
        log.info("初始化中间库的hashcode：" + connectionMDB.hashCode());

    }

    public void clear() throws SQLException {
        if (!ObjectUtil.equals(connectionMiddleDB, null))
            connectionMiddleDB.close();

        connectionMiddleDB = null;
    }



    private volatile DataSource dataSource_middle= null;
    public DataSource getDataSource_middle() throws Exception {

        if (ObjectUtil.equals(dataSource_middle, null)) {

            String url = ConfigUtils.CONFIG.getMiddle_database_url();
            String username = ConfigUtils.CONFIG.getMiddle_database_username();
            String password = ConfigUtils.CONFIG.getMiddle_database_password();
            Map map = new HashMap();
            map.put("url", url);
            map.put("username", username);
            map.put("password", password);
            dataSource_middle = DruidDataSourceFactory.createDataSource(map);
        }
        return dataSource_middle;
    }



    /**
     * 获取hutool的db
     * @param dataSource
     * @return
     */
    private Db getDb(DataSource dataSource){
        return Db.use(dataSource);
    }

    public Db getDb_middle() throws Exception {
        return GetInstance().getDb(GetInstance().getDataSource_middle());
    }

    //region 输出的中间库操作
    /**
     * 输出的中间库
     */
    static volatile Connection connectionMiddleDB = null;

    public Connection getMiddleDBConn() throws SQLException {

        if (ObjectUtil.equals(connectionMiddleDB, null)) {
            String url = ConfigUtils.CONFIG.getMiddle_database_url();
            String username = ConfigUtils.CONFIG.getMiddle_database_username();
            String password = ConfigUtils.CONFIG.getMiddle_database_password();
            connectionMiddleDB = DriverManager.getConnection(url, username, password);
            connectionMiddleDB.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connectionMiddleDB.setAutoCommit(false);
            return connectionMiddleDB;
        } else {

            connectionMiddleDB.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connectionMiddleDB.setAutoCommit(false);
            return connectionMiddleDB;
        }
    }




    //endregion
}
