package org.kiba.learning_data_stream;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.kiba.tools.job004.ClickEvent;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

/**
 * process使用3
 * reducingState使用
 */
public class Learning_DataStreamJob_012 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        long ts = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        DataStream<ClickEvent> text = env.fromElements(
                new ClickEvent("user1", 1, ts),  // 点击事件，包含用户 ID 和点击时间戳
                new ClickEvent("user1", 2, ts),
                new ClickEvent("user1", 3, ts),
                new ClickEvent("user1", 4, ts),  // 迟到的点击事件
                new ClickEvent("user1", 5, ts)
        );
        DataStream<ClickEvent> text2 = text.keyBy(p -> p.name).process(new ProcessFunction<ClickEvent, ClickEvent>() {
            ReducingStateDescriptor<Integer> reducingStateDescriptor = new ReducingStateDescriptor<Integer>(
                    "reducing-state",
                    new ReduceFunction<Integer>() {
                        @Override
                        public Integer reduce(Integer value1, Integer value2) {
                            return value1 + value2;
                        }
                    },
                    Integer.class
            );
            ReducingState<Integer> reducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
            }

            @Override
            public void processElement(ClickEvent value, ProcessFunction.Context ctx, Collector out) throws Exception {
                int nowTemperature = value.getData();
                if (nowTemperature % 2 > 0) {
                    reducingState.add(nowTemperature);
                    out.collect(value);
                } else {

                    value.setData(nowTemperature + reducingState.get());
                    out.collect(value);
                }
            }
        });
        text2.print();
        env.execute();
        Log.info("=====================================================================================================================");

    }
}
