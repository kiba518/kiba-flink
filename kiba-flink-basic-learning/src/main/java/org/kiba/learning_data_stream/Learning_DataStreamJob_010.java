package org.kiba.learning_data_stream;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.kiba.tools.job004.ClickEvent;
/**
 * process使用
 * ValueState使用
 */
public class Learning_DataStreamJob_010 {
    /**
     * 温度升高报警，这个例子不正确，应该定义一个状态数组[ListState]记录温度数据，在定义一个刻度时间，用当前时间判断是否大于刻度时间1秒，然后更新刻度时间，然后在注册时间事件，然后在时间事件里判断数组是否连续上升
     * 或者用聚合接口更好
     */
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
        text.keyBy(p -> p.name).process(new ProcessFunction<ClickEvent, String>() {
            ValueState<Double> temperature;
            ValueState<Long> currentTimeStamp;

            @Override
            public void open(Configuration parameters) throws Exception {
                //super.open(parameters);
                temperature = getRuntimeContext().getState(new ValueStateDescriptor<>("temp", Double.class, 0d));
                currentTimeStamp = getRuntimeContext().getState(new ValueStateDescriptor<>("currentTimeStamp", Long.class, 0l));
            }

            @Override
            public void processElement(ClickEvent value, ProcessFunction.Context ctx, Collector out) throws Exception {
                double preTempreature = temperature.value();
                double nowTemperature = Double.valueOf(value.getData());
                temperature.update(nowTemperature);
                long currentTime = currentTimeStamp.value();
                if (nowTemperature > preTempreature && currentTime == 0) {
                    long timerTs = ctx.timerService().currentProcessingTime() + 1000;
                    ctx.timerService().registerProcessingTimeTimer(timerTs);//处理时间 参数是毫秒 执行函数是onTimer
                    currentTimeStamp.update(timerTs);
                } else if (preTempreature > nowTemperature && preTempreature == 0) {//1秒中之内有下降的话，清除currentTimeStamp，删除
                    ctx.timerService().deleteEventTimeTimer(currentTime);
                    currentTimeStamp.clear();
                }

                //ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime()+1000);//事件时间 参数是毫秒
            }

            @Override
            public void onTimer(long timestamp, ProcessFunction.OnTimerContext ctx, Collector out) throws Exception {
                //super.onTimer(timestamp, ctx, out);
                out.collect("报警");
            }


        });
        text.print();
        env.execute();
        Log.info("=====================================================================================================================");

    }
}
