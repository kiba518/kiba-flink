package org.kiba.learning_data_stream;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * map的使用
 */
public class Learning_DataStreamJob_007 {

    public static void main(String[] args) throws Exception {


        Thread thread = new Thread(() -> {

            // 创建Flink作业的执行环境
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
            //env.setParallelism(4);//并发数量，默认即可
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);//使用window时添加该设置  TimeCharacteristic.ProcessingTime是窗口内部处理数据的时间 EventTime是数据创建时间 IngestionTime是进入flink的时间
            // 读取输入数据源，这里假设输入数据是以换行符分隔的文本文件
            DataStream<String> text = env.readTextFile("C:\\Project\\github\\kiba-flink\\kiba-flink-parent\\kiba-flink-basic-learning\\src\\main\\resources\\hello.txt");
            MapFunction mf = new MapFunction<String, String>() {//MapFunction<String,String> 输入和输出参数的类型
                @Override
                public String map(String value) throws Exception {
                    return value.toUpperCase();
                }
            };

            DataStream<String> words2 = text.map(mf);//map不支持keyby
            words2.print();
            Log.info("=====================================================================================================================");

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
