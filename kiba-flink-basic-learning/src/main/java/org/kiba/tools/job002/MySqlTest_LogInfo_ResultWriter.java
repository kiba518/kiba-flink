package org.kiba.tools.job002;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.kiba.utils.DatabaseConnectionUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class MySqlTest_LogInfo_ResultWriter extends RichSinkFunction<List<Test_LogInfo>> {
    private Connection connection = null;
    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DatabaseConnectionUtils.getWriterConn();
        ps = DatabaseConnectionUtils.getPreparedStatementWriter();

    }

    @Override
    public void invoke(List<Test_LogInfo> list, Context context) throws Exception {
        //获取JdbcReader发送过来的结果
        long startTime = System.currentTimeMillis();
        list.forEach((item) -> {
            try {
                ps.setInt(1, item.getId());
                ps.setString(2, item.getLoginfo());
                ps.setLong(3, item.getCreate_timestamp());
                ps.addBatch();
                //Log.info("invoke addBatch");

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        ps.executeBatch();
        connection.commit();
        //ps.clearBatch();

        DatabaseConnectionUtils.execution = true;
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        Log.info("===================================测试=======输出用时-毫秒。", String.valueOf(executionTime));
    }


    @Override
    public void close() throws Exception {
        super.close();
        if (ps != null) {
            ps.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}