package datastream.operators.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class tumblingWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = senv.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = socketTextStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] splited = value.split(",");
                String key = splited[0];
                Integer v = Integer.parseInt(splited[1]);
                return Tuple2.of(key,v);
            }
        });

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = map.keyBy(value -> value.f0)
                //size:窗口大小，offset:初始窗口的偏移量，默认0,比如5 seconds，那么从2021-05-10 10:21:05开始
                //窗口过期了才会打印
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));


        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = windowedStream.sum(1);



        sumed.print();

        senv.execute();

    }
}
