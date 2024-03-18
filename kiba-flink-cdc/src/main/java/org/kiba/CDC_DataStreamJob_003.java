package org.kiba;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * mongodb-cdc 参考 https://www.jianshu.com/p/f70f4e984e03
 */
public class CDC_DataStreamJob_003 {

    public static void main(String[] args) throws Exception {

        //创建Flink作业的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//设置流处理或者批处理，或者自动
        env.setParallelism(1);//并发数量，约等于开几个线程


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //关键字 要加` 这个符号，这个符号，不是单引号
        tableEnv.executeSql("CREATE TABLE blade_log_api (" +
                "_id STRING," +
                "createBy STRING," +
                "createTime STRING," +
                "serviceId STRING," +
                "serverIp STRING," +
                "serverHost STRING," +
                "env STRING," +
                "remoteIp STRING," +
                "userAgent STRING," +
                "requestUri STRING," +
                " `method` STRING," +
                "methodClass STRING," +
                "methodName STRING," +
                "params STRING," +
                "title STRING," +
                " `time` STRING," +
                "PRIMARY KEY(_id) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'mongodb-cdc'," +
//                "'scan.startup.mode'='initial', " +
                "'hosts' = '10.1.100.22:27017'," +
//                "'username' = ''," +
//                "'password' = ''," +
                "'database' = 'blade-log'," +
                "'collection' = 'blade_log_api'" +
                ")");
        Table table = tableEnv.sqlQuery("select * from blade_log_api");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);//将前面查询得到的 Table 对象转换为一个可撤回的数据流（RetractStream）
        retractStream.print();
//
//        tableEnv.executeSql("CREATE TABLE `etl_cdc_blade_log` (" +
//                "`_id` varchar(255) ," +
//                "`createBy` STRING, " +
//                "`createTime` STRING," +
//                "`serviceId` STRING," +
//                "`serverIp` STRING," +
//                "`serverHost` STRING," +
//                "`env` STRING," +
//                "`remoteIp` STRING," +
//                "`userAgent` STRING," +
//                "`requestUri` STRING, " +
//                "`method` STRING," +
//                "`methodClass` STRING," +
//                "`methodName` STRING," +
//                "`params` STRING," +
//                "`time` STRING, " +
//                "PRIMARY KEY(_id) NOT ENFORCED"+
//                ") WITH ( " +
//
//                "'connector'='jdbc', " +
//                "'url'='jdbc:mysql://10.1.0.145:3306/testdb1'," +
//                "'table-name'='etl_cdc_blade_log'," +
//                "'driver'='com.mysql.cj.jdbc.Driver'," +
//                "'username'='remote_user', " +
//                "'password'='123456', " +
//                "'scan.fetch-size'='200'" +
//                ")");
//
//
//        tableEnv.executeSql("insert into etl_cdc_blade_log select * from blade_log_api");



        env.execute("基础使用");
    }



}

/**
 * CREATE TABLE `etl_cdc_blade_log` (
 * 		`_id`varchar(255) ,
 * 		`createBy` text DEFAULT NULL,
 * 		`createTime` text DEFAULT NULL,
 * 		`serviceId` text DEFAULT NULL,
 * 		`serverIp` text DEFAULT NULL,
 * 		`serverHost` text DEFAULT NULL,
 * 		`env` text DEFAULT NULL,
 * 		`remoteIp` text DEFAULT NULL,
 * 		`userAgent` text DEFAULT NULL,
 * 		`requestUri` text DEFAULT NULL,
 * 		`method` text DEFAULT NULL,
 * 		`methodClass` text DEFAULT NULL,
 * 		`methodName` text DEFAULT NULL,
 * 		`params` text DEFAULT NULL,
 * 		`time` text DEFAULT NULL,
 * PRIMARY KEY (`_id`)
 *
 * ) ENGINE=InnoDBDEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='mongoDB日志cdc表';
 */