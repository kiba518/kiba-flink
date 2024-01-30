package org.kiba.learning_data_stream;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * partition未测试
 */
public class Learning_DataStreamJob_009_partition {

    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //env.setParallelism(4);//并发数量，默认即可
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//使用window时添加该设置  TimeCharacteristic.ProcessingTime 是窗口内部处理数据的时间 EventTime 是数据创建时间 IngestionTime 是进入flink的时间
        DataStream<String> text = env.readTextFile("C:\\Project\\github\\kiba-flink\\kiba-flink-parent\\kiba-flink-basic-learning\\src\\main\\resources\\hello.txt");


        env.execute("waterMark");
    }

    public static void partitionCustom(DataStream<Tuple2<Integer, String>> text) {
        // 定义自定义分区策略
        Partitioner<Tuple2<Integer, String>> partitioner = new Partitioner<Tuple2<Integer, String>>() {

            @Override
            public int partition(Tuple2<Integer, String> key, int numPartitions) {
                int tempi = key.f0 % 2;
                return tempi % numPartitions;
            }

        };

        // 使用 Partition 算子进行分区操作
        DataStream<Tuple2<Integer, String>> partition = text.partitionCustom(partitioner, 0);
        // 处理每个子流的数据并输出结果

        partition.addSink(new SinkFunction<Tuple2<Integer, String>>() {
            @Override
            public void invoke(Tuple2<Integer, String> value, Context context) throws Exception {
                SinkFunction.super.invoke(value, context);
                System.out.println("Partition: " + context + ", Value: " + value);
            }
        });

        Log.info("=====================================================================================================================");

    }
}
