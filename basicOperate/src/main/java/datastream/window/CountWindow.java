package datastream.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 按照key分组，划分countWindow
 */
public class CountWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据输入形式：word,num
        DataStreamSource<String> socket = senv.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = socket.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] splits = value.split(",");
                String key = splits[0];
                Integer num = Integer.parseInt(splits[1]);
                return Tuple2.of(key, num);
            }
        });


        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> countWindow = map.keyBy(0)
                .countWindow(5);
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = countWindow.sum(1);


        summed.print();
        senv.execute();

    }
}
