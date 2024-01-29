package org.kiba;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Properties;


public class CDC_DataStreamJob_001 {

    public static void main(String[] args) throws Exception {
        //创建Flink作业的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//设置流处理或者批处理，或者自动
        env.setParallelism(1);//并发数量，约等于开几个线程



        Properties dbProps = new Properties();
        dbProps.put("database.serverTimezone", "UTC");
        //创建 Flink-MySQL-CDC 的 Source
        //initial (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
        //latest-offset: Never to perform snapshot on the monitored database tables upon first  startup, just read from the end of the binlog which means only have the changes since the connector was started.
        //timestamp: Never to perform snapshot on the monitored database tables upon first  startup, and directly read binlog from the specified timestamp. The consumer will traverse the binlog from the beginning and ignore change events whose timestamp is smaller than the specified timestamp.
        //specific-offset: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified offset.
        MySqlSource mySqlSource = MySqlSource.<String>builder()
                .hostname("10.1.0.145")
                .port(3306)
                .databaseList("testdb1") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("testdb1.test_loginfo") // 设置捕获的表
                .username("remote_user")
                .password("123456")
                .debeziumProperties(dbProps)
                .deserializer(new CustomerDeserialization()) // 将 SourceRecord 转换为 JSON 字符串
                //.startupOptions(StartupOptions.initial())
                .startupOptions(StartupOptions.timestamp(1700739281)) // 从时间戳启动
                .build();


        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);

        DataStreamSource<String> dataStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(1);


        dataStream.addSink(new MysqlSink()).name("cdc");

        env.execute("基础使用");
    }



}
