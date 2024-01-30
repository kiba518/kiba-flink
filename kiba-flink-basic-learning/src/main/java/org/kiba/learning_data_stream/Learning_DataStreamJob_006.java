package org.kiba.learning_data_stream;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * flatMap的使用
 */
public class Learning_DataStreamJob_006 {

    public static void main(String[] args) throws Exception {


        Thread thread = new Thread(() -> {

            // 创建Flink作业的执行环境
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
            //env.setParallelism(4);//并发数量，默认即可
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);//使用window时添加该设置  TimeCharacteristic.ProcessingTime是窗口内部处理数据的时间 EventTime是数据创建时间 IngestionTime是进入flink的时间
            // 读取输入数据源，这里假设输入数据是以换行符分隔的文本文件
            DataStream<String> text = env.readTextFile("C:\\Project\\github\\kiba-flink\\kiba-flink-parent\\kiba-flink-basic-learning\\src\\main\\resources\\hello.txt");
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

            // 执行Flink作业
            try {
                env.execute("WordCount Example");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
    }

}
