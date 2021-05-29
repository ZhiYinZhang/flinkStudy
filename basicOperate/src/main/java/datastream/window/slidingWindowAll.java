package datastream.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class slidingWindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = senv.socketTextStream("localhost", 9999);


        SingleOutputStreamOperator<Integer> map = socketTextStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        });

        SingleOutputStreamOperator<Integer> summed = map.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(3)))
                .sum(0);

        summed.print();

        senv.execute();

    }
}
