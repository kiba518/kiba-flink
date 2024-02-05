package org.kiba.learning_data_stream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.kiba.tools.job004.ClickEvent;

import javax.swing.*;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * broadcast还是基于合流操作
 * 参考https://developer.aliyun.com/article/706760，但这文章写的内容不成立，只能借鉴
 */
public class Learning_DataStreamJob_017_broadcast {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);//在执行命令中使用：--name 张三 --age 20


        ParameterTool parameter_system = ParameterTool.fromSystemProperties();//从系统属性中获取,当启动一个JVM时，你可以给它传递一些系统属性，如：-Dinput=hdfs:///mydata，你可以用这些系统属性来初始化 PrameterTool

        InputStream in = Learning_DataStreamJob_017_broadcast.class.getResourceAsStream("/log4j2.properties");
        ParameterTool parameter_properties = ParameterTool.fromPropertiesFile(in);   // properties


        parameter_properties.getNumberOfParameters(); // 参数个数
        String level = parameter_properties.getRequired("rootLogger.level");




        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);//设置输入流并行度


        DataStream<Integer> dataStreamSource1 = env.fromElements(1, 10, 50);

        ArrayList<Tuple2<String,Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("zs", 23));
        broadData.add(new Tuple2<>("ls", 34));
        broadData.add(new Tuple2<>("ww", 45));
        List<String> list= new ArrayList<>();


        DataStream<Tuple2<String, Integer>> dataStreamSource2 = env.fromCollection(broadData);


        DataStream<String> dataStreamSource3 = env.fromElements("zs", "ls", "ww");



        MapStateDescriptor<Void, Integer> bcStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.GENERIC(Integer.class));

        BroadcastStream<Integer> bcedPatterns = dataStreamSource1.broadcast(bcStateDescriptor);

        DataStream<Object> matches = dataStreamSource2
                .connect(bcedPatterns)
                .process(new BroadcastProcessFunction<Tuple2<String, Integer>, Integer, Object>() {

                    // broadcast state descriptor
                    MapStateDescriptor<Void, Integer> patternDesc;

                    @Override
                    public void open(Configuration conf) {
                        // initialize keyed state

                        patternDesc = new MapStateDescriptor<>("patterns", Types.VOID, Types.GENERIC(Integer.class));
                    }
                    @Override
                    public void processElement(Tuple2<String, Integer> value, BroadcastProcessFunction<Tuple2<String, Integer>, Integer, Object>.ReadOnlyContext ctx, Collector<Object> out) throws Exception {
                        Integer pattern = ctx.getBroadcastState(this.patternDesc).get(null);


                    }

                    @Override
                    public void processBroadcastElement(Integer value, BroadcastProcessFunction<Tuple2<String, Integer>, Integer, Object>.Context ctx, Collector<Object> out) throws Exception {
                        // store the new pattern by updating the broadcast state
                        BroadcastState<Void, Integer> bcState = ctx.getBroadcastState(patternDesc);
                        // storing in MapState with null as VOID default value
                        if(value.equals(1)) {
                            bcState.put(null, value);
                        }
                    }
                });

















        matches.print();

        env.execute();
    }
}
