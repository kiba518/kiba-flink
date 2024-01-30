package org.kiba;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.kiba.tools.job_001.CustomerDeserialization;
import org.kiba.tools.job_001.MysqlSink;
import org.kiba.utils.ConfigUtils;

import java.util.Properties;

/**
 * 启动模式
 * 配置选项scan.startup.mode指定 MySQL CDC 使用者的启动模式。有效枚举包括：
 * initial （默认）：在第一次启动时对受监视的数据库表执行初始快照，并继续读取最新的 binlog。
 * earliest-offset：跳过快照阶段，从可读取的最早 binlog 位点开始读取
 * latest-offset：首次启动时，从不对受监视的数据库表执行快照， 连接器仅从 binlog 的结尾处开始读取，这意味着连接器只能读取在连接器启动之后的数据更改。
 * specific-offset：跳过快照阶段，从指定的 binlog 位点开始读取。位点可通过 binlog 文件名和位置指定，或者在 GTID 在集群上启用时通过 GTID 集合指定。
 * timestamp：跳过快照阶段，从指定的时间戳开始读取 binlog 事件。

 * initial (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
 * latest-offset: Never to perform snapshot on the monitored database tables upon first  startup, just read from the end of the binlog which means only have the changes since the connector was started.
 * timestamp: Never to perform snapshot on the monitored database tables upon first  startup, and directly read binlog from the specified timestamp. The consumer will traverse the binlog from the beginning and ignore change events whose timestamp is smaller than the specified timestamp.
 * specific-offset: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified offset.
 */
public class CDC_DataStreamJob_001 {

    public static void main(String[] args) throws Exception {
        //创建Flink作业的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//设置流处理或者批处理，或者自动
        env.setParallelism(1);//并发数量，约等于开几个线程


        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //指定checkpoint的语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //设置可容忍的检查点失败数，默认值为0表示不允许容忍任何检查点失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        //设置任务关闭的时候保留最后一次checkpoint的数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //指定从Checkpoint自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        env.getCheckpointConfig().setCheckpointStorage("file:///c:/ckpt");

        Properties dbProps = new Properties();
        dbProps.put("database.serverTimezone", "UTC");
        //dbProps.put("database.serverTimezone", "Asia/Shanghai");//MYSQL是cst时用这个配置 使用命令查询 select @@system_time_zone;


        final String host = ConfigUtils.CONFIG.getHost();
        final int port = ConfigUtils.CONFIG.getPort();
        final String username = ConfigUtils.CONFIG.getMiddle_database_username();
        final String password = ConfigUtils.CONFIG.getMiddle_database_password();
        final String dbName = ConfigUtils.CONFIG.getDb_name();


        MySqlSource mySqlSource = MySqlSource.<String>builder()
                .hostname(host)
                .port(port)
                .databaseList(dbName) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList(dbName+".test_loginfo_new") // 设置捕获的表
                .username(username)
                .password(password)
                .debeziumProperties(dbProps)
                .deserializer(new CustomerDeserialization()) // 将 SourceRecord 转换为 JSON 字符串
                .startupOptions(StartupOptions.initial())
                //.startupOptions(StartupOptions.timestamp(1700739281)) // 从时间戳启动
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
