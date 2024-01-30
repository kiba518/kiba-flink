package org.kiba.learning_data_stream;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.kiba.tools.job004.ClickEvent;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

/**
 * waterMark的使用
 */
public class Learning_DataStreamJob_008 {

    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //env.setParallelism(4);//并发数量，默认即可
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//使用window时添加该设置  TimeCharacteristic.ProcessingTime 是窗口内部处理数据的时间 EventTime 是数据创建时间 IngestionTime 是进入flink的时间
        LocalDateTime now = LocalDateTime.now();
        long timestamp = now.toEpochSecond(ZoneOffset.UTC);

        System.out.println("当前时间戳: " + timestamp);
        // 创建一个数据源，生成点击事件数据
        DataStream<ClickEvent> input = env.fromElements(
                new ClickEvent("user1", 1, timestamp),  // 点击事件，包含用户 ID 和点击时间戳
                new ClickEvent("user1", 2, timestamp),
                new ClickEvent("user2", 3, timestamp),
                new ClickEvent("user1", 4, timestamp),  // 迟到的点击事件
                new ClickEvent("user2", 5, timestamp)
        );

        //TumblingEventTimeWindows 时间滚动窗口
        //SlidingEventTimeWindows 时间滑动窗口

        DataStream<ClickEvent> result = input
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))//设置允许乱序最大间隔5秒
                                .<String, Integer>withTimestampAssigner((event, timestamp1) -> event.getTimestamp())
                )//要先设置watermark，后设置keyby
                .keyBy(p -> p.name)
                //.timeWindow(Time.of(5, TimeUnit.SECONDS), Time.of(2, TimeUnit.SECONDS)).allowedLateness(Time.of(2, TimeUnit.SECONDS))// 5秒钟窗口，每2秒滑动一次,允许2秒网络延迟
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2))).allowedLateness(Time.of(2, TimeUnit.SECONDS))  // 滑动窗口，窗口大小为 5 秒，滑动步长为 2 秒,允许2秒网络延迟
                .reduce((value1, value2) -> new ClickEvent(value2.name, value2.data + value1.data, value2.getTimestamp()));  // 计算每个窗口内的点击次数总和


        result.print();
        env.execute("waterMark");
    }


}
