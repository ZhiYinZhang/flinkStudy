package datastream.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class sessionWindow {
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


        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = map.keyBy(value -> value.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
                .sum(1);

        summed.print();
        senv.execute();
    }
}
