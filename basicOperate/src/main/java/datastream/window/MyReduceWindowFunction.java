package datastream.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class MyReduceWindowFunction {
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

        SingleOutputStreamOperator<Tuple2<String, Integer>> process = map
                .keyBy(r -> r.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String,Integer>>(){

                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0,value1.f1+value2.f1);
                    }
                });


        process.print();
        senv.execute();
    }
}
