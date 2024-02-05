package org.kiba;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * 未测试
 */
public class CDC_DataStreamJob_002 {

    public static void main(String[] args) throws Exception {
        //创建Flink作业的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//设置流处理或者批处理，或者自动
        env.setParallelism(1);//并发数量，约等于开几个线程

        Properties dbProps = new Properties();
        dbProps.put("database.serverTimezone", "UTC");


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //使用Flink SQL方式构建CDC表
        tableEnv.executeSql("CREATE TABLE test_loginfo ( " +
                "id INT , " +
                "loginfo STRING, " +
                "create_timestamp TIMESTAMP(0), " +
                "PRIMARY KEY(id) NOT ENFORCED"+
                ") WITH ( " +
                "'connector'='mysql-cdc', " +
                "'scan.startup.mode'='latest-offset', " +
                "'hostname'='10.1.0.145', " +
                "'port'='3306', " +
                "'username'='remote_user', " +
                "'password'='123456', " +
                "'database-name'='testdb1', " +
                "'table-name'='testdb1.test_loginfo' " +
                ")");
        Table table = tableEnv.sqlQuery("select * from test_loginfo");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();



        env.execute("基础使用");
    }



}
