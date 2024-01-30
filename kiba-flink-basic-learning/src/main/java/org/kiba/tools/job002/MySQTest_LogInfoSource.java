package org.kiba.tools.job002;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.kiba.utils.DatabaseConnectionUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MySQTest_LogInfoSource implements SourceFunction<List<Test_LogInfo>> {
    private boolean isRunning = true;
    // 连接 MySQL 数据库
    Connection connection;
    PreparedStatement preparedStatement;
    long lastUpdateTimestamp;


    @Override
    public void run(SourceContext<List<Test_LogInfo>> sourceContext) throws Exception {

        connection = DatabaseConnectionUtils.getReaderConn();
        preparedStatement = connection.prepareStatement("SELECT * FROM test_loginfo WHERE create_timestamp > ? order by id  ");
        lastUpdateTimestamp = getLastUpdateTimestamp(); // 获取上次更新的时间戳
        connection.setAutoCommit(false);

        while (isRunning) {
            DatabaseConnectionUtils.execution = false;
            long startTime = System.currentTimeMillis();
            preparedStatement.setLong(1, lastUpdateTimestamp); //初始时间时间戳

            // 查询增量数据
            ResultSet resultSet = preparedStatement.executeQuery();
            List<Test_LogInfo> list = new ArrayList<>();

            while (resultSet.next() && isRunning) {

                long currentTimestamp = resultSet.getLong("create_timestamp");
                if (currentTimestamp > lastUpdateTimestamp) {
                    lastUpdateTimestamp = currentTimestamp;
                }
                Test_LogInfo loginfo = new Test_LogInfo();

                int column1 = resultSet.getInt("id");
                String column2 = resultSet.getString("loginfo");
                long column3 = resultSet.getLong("create_timestamp");

                loginfo.setId(column1);
                loginfo.setLoginfo(column2);
                loginfo.setCreate_timestamp(column3);
                list.add(loginfo);
            }
            resultSet.close();
            //preparedStatement.close();
            connection.commit();
            sourceContext.collect(list);
            // 关闭连接和语句对象
            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;
            Log.info("读取用时-毫秒。", String.valueOf(executionTime));

            Log.info("===读取list的size：" + list.size()+" === 执行sql"+preparedStatement.toString());
            if (list.size() == 0) {
                DatabaseConnectionUtils.execution = true;
                Log.info("==========================================未读取到数据==========================================");
            }
            while (DatabaseConnectionUtils.execution == false && isRunning) {
                Thread.sleep(100);
            }

            Thread.sleep(10000);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
        try {
            preparedStatement.close();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long getLastUpdateTimestamp() {
        return 1699374727;
    }
}