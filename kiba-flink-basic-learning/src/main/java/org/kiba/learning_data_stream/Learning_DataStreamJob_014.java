package org.kiba.learning_data_stream;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.kiba.tools.job004.ClickEvent;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * 没处理完 侧流输出
 */
public class Learning_DataStreamJob_014 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        long ts = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        DataStream<ClickEvent> text = env.fromElements(
                new ClickEvent("user1", 1, ts),  // 点击事件，包含用户 ID 和点击时间戳
                new ClickEvent("user1", 2, ts),
                new ClickEvent("user2", 3, ts),
                new ClickEvent("user1", 4, ts),  // 迟到的点击事件
                new ClickEvent("user2", 5, ts)
        );

        DataStream<String> text2 = text.keyBy(p -> p.name).process(new ProcessFunction<ClickEvent, String>() {
            @Override
            public void processElement(ClickEvent value, ProcessFunction.Context ctx, Collector out) throws Exception {
                double nowTemperature = Double.valueOf(value.getData());
                if (nowTemperature > 2) {

                } else {//1秒中之内有下降的话，清除currentTimeStamp，删除
                    out.collect(value);
                }

                //ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime()+1000);//事件时间 参数是毫秒
            }
        });

        env.execute();
        Log.info("=====================================================================================================================");
    }
}
