package org.kiba.learning_data_stream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * 全量迭代
 */
public class Learning_DataStreamJob_016_param {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);//在执行命令中使用：--name 张三 --age 20


        ParameterTool parameter_system = ParameterTool.fromSystemProperties();//从系统属性中获取,当启动一个JVM时，你可以给它传递一些系统属性，如：-Dinput=hdfs:///mydata，你可以用这些系统属性来初始化 PrameterTool

        InputStream in = Learning_DataStreamJob_016_param.class.getResourceAsStream("/log4j2.properties");
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

        dataStreamSource1.connect(dataStreamSource2).process(new CoProcessFunction<Integer, Tuple2<String, Integer>, Object>() {
            @Override
            public void processElement1(Integer value, CoProcessFunction<Integer, Tuple2<String, Integer>, Object>.Context ctx, Collector<Object> out) throws Exception {

                out.collect(value);//1流输入触发
            }

            @Override
            public void processElement2(Tuple2<String, Integer> value, CoProcessFunction<Integer, Tuple2<String, Integer>, Object>.Context ctx, Collector<Object> out) throws Exception {
                out.collect(value);//2流输入触发
            }
        });

        dataStreamSource1.print();

        env.execute();
    }
}
