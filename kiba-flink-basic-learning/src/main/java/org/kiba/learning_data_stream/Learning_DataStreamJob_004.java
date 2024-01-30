package org.kiba.learning_data_stream;

import cn.hutool.core.date.DateUtil;
import com.esotericsoftware.minlog.Log;
import com.mongodb.client.*;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.*;
import org.kiba.tools.job004.ClickEvent;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;

/**
 * reduce的使用
 */
public class Learning_DataStreamJob_004 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //env.setParallelism(4);//并发数量，默认即可
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//使用window时添加该设置  TimeCharacteristic.ProcessingTime 是窗口内部处理数据的时间 EventTime 是数据创建时间 IngestionTime 是进入flink的时间
        LocalDateTime now = LocalDateTime.now();
        long timestamp = now.toEpochSecond(ZoneOffset.UTC);

        System.out.println("当前时间戳: " + timestamp);
        DataStream<ClickEvent> input = env.fromElements(
                new ClickEvent("user1", 1, timestamp),  // 点击事件，包含用户 ID 和点击时间戳
                new ClickEvent("user1", 2, timestamp),
                new ClickEvent("user2", 3, timestamp),
                new ClickEvent("user1", 4, timestamp),  // 迟到的点击事件
                new ClickEvent("user2", 5, timestamp),
                new ClickEvent("user1", 4, timestamp),  // 迟到的点击事件
                new ClickEvent("user2", 5, timestamp),
                new ClickEvent("user1", 4, timestamp),  // 迟到的点击事件
                new ClickEvent("user2", 5, timestamp),
                new ClickEvent("user1", 4, timestamp),  // 迟到的点击事件
                new ClickEvent("user2", 5, timestamp),
                new ClickEvent("user1", 4, timestamp),  // 迟到的点击事件
                new ClickEvent("user2", 5, timestamp),
                new ClickEvent("user1", 4, timestamp),  // 迟到的点击事件
                new ClickEvent("user2", 5, timestamp)
        );
        // 对单词流进行分组并计数，输出结果到控制台
        input.keyBy(p->p.name)
                .reduce((word1, word2) -> new ClickEvent(word1.name, word1.data + word2.data,word2.getTimestamp()))//依次两两配对
                .print();
        Log.info("=====================================================================================================================");

        env.execute("reduce");
    }






}
