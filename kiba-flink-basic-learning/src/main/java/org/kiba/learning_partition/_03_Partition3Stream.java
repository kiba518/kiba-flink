package org.kiba.learning_partition;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 未测试
 *
 */

public class _03_Partition3Stream {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);//rest.port 是用于配置 Flink 监控页面端口的参数，默认值为8081。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);


        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///e:/ckptwangzhenguangshigedashabi");

        // 构造数据流1  socket传递来的数据格式为：id,name
        DataStreamSource<String> streamSource1 = env.socketTextStream("hadooop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, String>> stream1 = streamSource1.map(s -> {
            String[] split = s.split(",");
            return Tuple2.of(split[0], split[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {});

        //构造数据流2  socket传递来的数据格式为：id,age,city
        DataStreamSource<String> streamSource2 = env.socketTextStream("hadoop102", 9998);
        SingleOutputStreamOperator<Tuple3<String, String, String>> stream2 = streamSource2.map(s -> {
            String[] split = s.split(",");
            return Tuple3.of(split[0], split[1], split[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {});

        /**
         * 广播变量的使用
         */
        MapStateDescriptor<String, Tuple2<String, String>> userInfoBroadcast = new MapStateDescriptor<>("userInfoStateDesc", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }));
        BroadcastStream<Tuple3<String, String, String>> broadcastStream = stream2.broadcast(userInfoBroadcast);


        stream1.connect(broadcastStream).process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {

            /**
             * 本方法，是用来处理 主流中的数据（每来一条，调用一次）
             * @param element  左流（主流）中的一条数据
             * @param ctx  上下文
             * @param out  输出器
             * @throws Exception
             */
            @Override
            public void processElement(Tuple2<String, String> element, ReadOnlyContext ctx, Collector<String> out) throws Exception {

                ReadOnlyBroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(userInfoBroadcast);

                if(broadcastState!=null){
                    Tuple2<String, String> userInfo = broadcastState.get(element.f0);
                    //代码健壮性，判空操作
                    out.collect(element.f0 + "," + element.f1 + "," + (userInfo == null ? null : userInfo.f0) + "," + (userInfo == null ? null : userInfo.f1));
                }else {
                    out.collect(element.f0+","+element.f1+ "," + null + "," + null);
                }
            }

            /**
             *
             * @param element  广播流中的一条数据
             * @param ctx  上下文
             * @param out 输出器
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(Tuple3<String, String, String> element, Context ctx, Collector<String> out) throws Exception {

                // 从上下文中，获取广播状态对象（可读可写的状态对象）
                BroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(userInfoBroadcast);

                // 然后将获得的这条  广播流数据， 拆分后，装入广播状态
                broadcastState.put(element.f0,Tuple2.of(element.f1,element.f2));

            }
        });
    }
}



