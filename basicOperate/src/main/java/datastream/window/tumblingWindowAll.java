package datastream.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class tumblingWindowAll {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = senv.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> map = socketTextStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                Integer num = Integer.parseInt(value);
                return num;
            }
        });

        AllWindowedStream<Integer, TimeWindow> windowAll = map.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Integer> summed = windowAll.sum(0);

        summed.print();


        senv.execute();


    }
}
