package org.kiba.learning_data_stream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.kiba.tools.job004.ClickEvent;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * broadcast还是基于合流操作
 * 参考https://developer.aliyun.com/article/706760，但这文章写的内容不成立，只能借鉴
 */
public class Learning_DataStreamJob_018_broadcast_pojo {

    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);//设置输入流并行度


        ArrayList<Tuple2<String,Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("zs", 23));
        broadData.add(new Tuple2<>("ls", 34));
        broadData.add(new Tuple2<>("ww", 45));
        DataStream<Tuple2<String, Integer>> dataStreamSource2 = env.fromCollection(broadData);

        long ts = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        DataStream<ClickEvent> dataStreamSource1 = env.fromElements(
                new ClickEvent("user1", 1, ts),  // 点击事件，包含用户 ID 和点击时间戳
                new ClickEvent("user1", 2, ts),
                new ClickEvent("user2", 3, ts),
                new ClickEvent("user1", 4, ts),  // 迟到的点击事件
                new ClickEvent("user2", 5, ts)
        );


        MapStateDescriptor<Void, ClickEvent[]> bcStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.OBJECT_ARRAY(Types.POJO(ClickEvent.class)));

        BroadcastStream<ClickEvent> bcedPatterns = dataStreamSource1.broadcast(bcStateDescriptor);

        DataStream<Object> matches = dataStreamSource2
                .connect(bcedPatterns).process(new BroadcastProcessFunction<Tuple2<String, Integer>, ClickEvent, Object>() {

                    MapStateDescriptor<Void, ClickEvent> patternDesc;
                    @Override
                    public void open(Configuration conf) {
                        // initialize keyed state

                        patternDesc = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(ClickEvent.class));
                    }
                    @Override
                    public void processElement(Tuple2<String, Integer> value, BroadcastProcessFunction<Tuple2<String, Integer>, ClickEvent, Object>.ReadOnlyContext ctx, Collector<Object> out) throws Exception {

                    }

                    @Override
                    public void processBroadcastElement(ClickEvent value, BroadcastProcessFunction<Tuple2<String, Integer>, ClickEvent, Object>.Context ctx, Collector<Object> out) throws Exception {
                        BroadcastState<Void, ClickEvent> bcState = ctx.getBroadcastState(patternDesc);
                        bcState.put(null, value);

                    }
                });


















        matches.print();

        env.execute();
    }
}
