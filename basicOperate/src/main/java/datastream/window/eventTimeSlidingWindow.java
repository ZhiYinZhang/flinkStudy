package datastream.window;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class eventTimeSlidingWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //flink1.12不需要设置这个，弃用了
//        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        数据格式：1622078719000,spark,1
        DataStreamSource<String> socketTextStream = senv.socketTextStream("localhost", 9999);


        //指定数据中时间数据
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))  //不设置延时，注意前面的String，输入数据类型
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() { //TimestampAssigner的参数类型和前面对应
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        //需要将时间转成timestamp，即long类型的毫秒值
                        String s = element.split(",")[0];
                        return Long.parseLong(s);
                    }
                });

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> process = socketTextStream
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] split = value.split(",");

                        return Tuple2.of(split[1], Integer.parseInt(split[2]));
                    }
                })
                .keyBy(x -> x.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(5)))
                .process(new MyProcessWindowFunction());
//                .sum(1);

        process.print();
        senv.execute();
    }
}
