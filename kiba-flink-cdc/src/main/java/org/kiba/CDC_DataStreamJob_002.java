package org.kiba;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * flink-table-planner：planner 计划器，是table API 最主要的部分，提供了运行时环境和生成程序执行计划的planner；
 * flink-table-api-scala-bridge：bridge 桥接器，主要负责table API 和DataStream/DataSet API的连接支持，按照语言分java 和scala。
 * 这里的两个依赖，是IDE 环境下运行需要添加的；如果是生产环境，lib 目录下默认已经有了planner，就只需要有bridge 就可以了。
 * 如果想使用用户自定义函数，或是跟kafka 做连接，需要有一个SQL client，这个包含在flink-table-common 里。
 * flink-connector-jdbc_2.12是 Flink 的 JDBC 连接器，用于与数据库进行交互。
 */
public class CDC_DataStreamJob_002 {

    public static void main(String[] args) throws Exception {

//        try {
//            Class.forName("com.mysql.cj.jdbc.Driver");//加载数据库驱动
//        }
//        catch (Exception e){
//            Log.error(e.getMessage());
//        }
//        try {
//            Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
//        }
//        catch (Exception e){
//            Log.error(e.getMessage());
//        }
        //创建Flink作业的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//设置流处理或者批处理，或者自动
        env.setParallelism(1);//并发数量，约等于开几个线程

//        Properties dbProps = new Properties();
//        dbProps.put("database.serverTimezone", "UTC");


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //使用Flink SQL方式构建CDC表
        tableEnv.executeSql("CREATE TABLE test_loginfo ( " +
                "id INT , " +
                "loginfo STRING, " +
                "create_timestamp bigint, " +
                "PRIMARY KEY(id) NOT ENFORCED"+
                ") WITH ( " +
                "'server-time-zone'='UTC',"+
                "'connector'='mysql-cdc', " +
                //"'scan.startup.mode'='latest-offset', " +
                "'scan.startup.mode'='initial', " +
                "'hostname'='10.1.0.145', " +
                "'port'='3306', " +
                "'username'='remote_user', " +
                "'password'='123456', " +
                "'database-name'='testdb1', " +
                "'table-name'='test_loginfo' " +
                ")");


//        Table table = tableEnv.sqlQuery("select * from test_loginfo");
//        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);//将前面查询得到的 Table 对象转换为一个可撤回的数据流（RetractStream）
//        retractStream.print();


        tableEnv.executeSql("CREATE TABLE test_loginfo_copy1 ( " +
                "id INT , " +
                "loginfo STRING, " +
                "create_timestamp bigint, " +
                "PRIMARY KEY(id) NOT ENFORCED"+
                ") WITH ( " +

                "'connector'='jdbc', " +
                "'url'='jdbc:mysql://10.1.0.145:3306/testdb1'," +
                "'table-name'='test_loginfo_copy1'," +
                //"'driver'='com.alibaba.druid.proxy.DruidDriver'," +
                "'driver'='com.mysql.cj.jdbc.Driver'," +
                "'username'='remote_user', " +
                "'password'='123456', " +
                "'scan.fetch-size'='200'" +
                ")");


        tableEnv.executeSql("insert into test_loginfo_copy1 select * from test_loginfo");



        env.execute("基础使用");
    }



}
