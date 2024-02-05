package org.kiba.learning_data_stream;

import cn.hutool.core.util.ObjectUtil;
import com.esotericsoftware.minlog.Log;
import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.kiba.tools.job002.*;
import org.kiba.utils.DatabaseConnectionUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 自定义数据源，读取数据，并存储到新表
 */
public class Learning_DataStreamJob_002 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                3, // 尝试重启的次数
//                Time.of(2, TimeUnit.SECONDS) // 间隔
//        ));
        env.setParallelism(16);//设置输入流并行度
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //region 测试转存
        DataStreamSource<List<Test_LogInfo>> dataStream = env.addSource(new MySQTest_LogInfoSource());
        dataStream.addSink(new MySqlTest_LogInfo_ResultWriter());
        //endregion
        //dataStream.print();
        env.execute("Flink DataTestStreamJob");
    }






}
