package datastream.operators.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class CountWindowAll {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //数据:
        //10
        //9
        DataStreamSource<String> socket = senv.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> map = socket.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        });

        //全局的count窗口，每5条一个窗口
        AllWindowedStream<Integer, GlobalWindow> countWindowAll = map.countWindowAll(5);

        //求和
        SingleOutputStreamOperator<Integer> summed = countWindowAll.sum(0);
        summed.print();
        senv.execute();

    }
}
