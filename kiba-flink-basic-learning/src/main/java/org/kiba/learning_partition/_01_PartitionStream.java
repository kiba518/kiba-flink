package org.kiba.learning_partition;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 未测试
 * 参考：https://blog.csdn.net/weixin_44244088/article/details/131316803
 */
public class _01_PartitionStream {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);//rest.port 是用于配置 Flink 监控页面端口的参数，默认值为8081。
        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.141.141", 9000);
        DataStream<String> map1Ds = dataStreamSource.map(x -> "demo" + x).setParallelism(12);

        DataStream<String> flatMapDS = map1Ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
            }
        }).setParallelism(2);

        DataStream<String> map2Ds = flatMapDS.map(x -> x + ".txt" + ":" + new Random().nextInt(10)).setParallelism(4);

        DataStream<String> processed = map2Ds.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value + "xxx";
            }
        }).process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out)
                    throws Exception {
                out.collect(value.split(":")[0]);
            }
        }).setParallelism(4);

        DataStream<String> filteDS = processed.filter(x -> x.length() % 2 == 0).setParallelism(4);

        filteDS.print().setParallelism(2);

        env.execute();

    }
}


