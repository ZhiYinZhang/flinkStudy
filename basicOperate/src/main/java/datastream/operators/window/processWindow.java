package datastream.operators.window;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class processWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> socketTextStream = senv.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = socketTextStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] splited = value.split(",");
                String key = splited[0];
                Integer v = Integer.parseInt(splited[1]);
                return Tuple2.of(key,v);
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> process = map
                .keyBy(r -> r.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(3)))
                .process(new MyProcessWindowFunction());


        process.print();
        senv.execute();
    }
}
