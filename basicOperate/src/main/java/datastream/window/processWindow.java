package datastream.window;


import datastream.window.MyProcessWindowFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class processWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> socketTextStream = senv.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> map = socketTextStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> process = map.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(6)))
                .process(new MyProcessWindowFunction());
//                .sum(0);

//        summed.print();

        process.print();
        senv.execute();
    }
}
