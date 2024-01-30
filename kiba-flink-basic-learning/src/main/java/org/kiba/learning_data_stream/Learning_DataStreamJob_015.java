package org.kiba.learning_data_stream;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.kiba.tools.job004.ClickEvent;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * 全量迭代
 */
public class Learning_DataStreamJob_015 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);//设置输入流并行度
        DataStream<Integer> dataStreamSource = env.fromElements(1, 10, 50);

        IterativeStream<Integer> iterativeDataSet = dataStreamSource.iterate();// 构建迭代头
        iterativeDataSet.setParallelism(1);//设置反馈流并行度
        // 对迭代集合并进行全量迭代操作，计算每个数字的平方和
        DataStream<Integer> squaredSums = iterativeDataSet
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value + 1;
                    }
                });
        DataStream<Integer> filters = squaredSums.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value < 100;
            }
        });
        iterativeDataSet.print();

        iterativeDataSet.closeWith(filters);

        env.execute();
    }
}
