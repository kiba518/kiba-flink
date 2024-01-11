package org.kiba;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class Learning_DataStreamJob_001 {

    public static void main(String[] args) throws Exception {
        //创建Flink作业的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//设置流处理或者批处理，或者自动
        env.setParallelism(1);//并发数量，约等于开几个线程
        //使用window时，可以选择使用EventTime或者ProcessingTime，EventTime主要是配合watermark使用。
        //TimeCharacteristic.ProcessingTime是窗口内部处理数据的时间 EventTime是数据创建时间 IngestionTime是进入flink的时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 读取输入数据源，这里假设输入数据是以换行符分隔的文本文件
        DataStream<String> text = env.readTextFile("hello.txt");
        FlatMapFunction<String, Tuple2<String, Integer>> fmf = new FlatMapFunction<String, Tuple2<String, Integer>>() {//FlatMapFunction<String,String> 输入和输出参数的类型
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split("\\s+"); // 将字符串拆分为单词,依据空格拆分
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1)); // 输出单词及其出现的次数
                }
            }

        };

        // 将输入数据源转换为一个单词流
        DataStream<Tuple2<String, Integer>> words = text.flatMap(fmf);

        // 对单词流进行分组并计数，输出结果到控制台
        words.keyBy(0)
                .reduce((word1, word2) -> new Tuple2<String, Integer>(word1.f0, word1.f1 + word2.f1 * 10))//依次两两配对
                .print();
        Log.info("=====================================================================================================================");
        words.keyBy(0)
                .sum(1)//聚合sum第二个数
                .print();

        env.execute("基础使用");
    }
}
