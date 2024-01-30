package org.kiba.learning_data_stream;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
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

/**
 * process使用3
 * aggregatingState使用
 */
public class Learning_DataStreamJob_013 {

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
            /**
             * AggregatingStateDescriptor可以使用的 aggFunction
             * SumFunction：用于计算元素的总和。
             * MinFunction：用于计算元素的最小值。
             * MaxFunction：用于计算元素的最大值。
             * AverageFunction：用于计算元素的平均值。
             * CountFunction：用于计算元素的数量。
             * RichAggregateFunction 自定义
             */
            AggregatingStateDescriptor<Integer, Integer, Integer> aggregatingStateDescriptor = new AggregatingStateDescriptor<Integer, Integer, Integer>(
                    "aggregating-state",
                    new RichAggregateFunction<Integer, Integer, Integer>() {
                        Integer x=0;
                        @Override
                        public Integer createAccumulator() { //累加器，统计当前是第几个元素
                            return 0;
                        }

                        @Override
                        public Integer add(Integer value, Integer accumulator) {
                            x=value;
                            return value;
                        }

                        @Override
                        public Integer getResult(Integer accumulator) {
                            return accumulator+x;
                        }

                        @Override
                        public Integer merge(Integer a, Integer b) {
                            return 0;
                        }
                    },
                    Integer.class
            );
            AggregatingState<Integer, Integer> aggState;

            @Override
            public void open(Configuration parameters) throws Exception {
                aggState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);
            }

            @Override
            public void processElement(ClickEvent value, ProcessFunction.Context ctx, Collector out) throws Exception {
                int nowTemperature = value.getData();
                aggState.add(nowTemperature);

                value.setData(aggState.get());
                out.collect(value);
//                if (nowTemperature % 2 > 0) {
//                    aggState.add(nowTemperature);
//                    out.collect(value);
//                } else {
//
//                    value.setData(nowTemperature+aggState.get());
//                    out.collect(value);
//                }
            }
        });
        text2.print();
        env.execute();
        Log.info("=====================================================================================================================");
    }
}
