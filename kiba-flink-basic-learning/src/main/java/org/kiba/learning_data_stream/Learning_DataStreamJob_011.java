package org.kiba.learning_data_stream;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
 * process使用2
 * ListState使用
 */
public class Learning_DataStreamJob_011 {

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
            ListState<Integer> temperatureList;

            @Override
            public void open(Configuration parameters) throws Exception {
                //super.open(parameters);
                temperatureList = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("temperature", Integer.class));
            }

            @Override
            public void processElement(ClickEvent value, ProcessFunction.Context ctx, Collector out) throws Exception {
                int nowTemperature = value.getData();

                if (nowTemperature % 2 > 0) {
                    temperatureList.add(nowTemperature);
                    out.collect(value);
                } else {
                    AtomicReference<Integer> sum1 = new AtomicReference<>((Integer) 0);
                    temperatureList.get().forEach(item -> {
                        sum1.set(sum1.get() + item);
                    });
                    value.setData(nowTemperature + sum1.get());
                    out.collect(value);
                }
            }
        });
        text2.print();
        env.execute();
        Log.info("=====================================================================================================================");

    }
}
