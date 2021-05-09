package datastream.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;

public class java_addSink{
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements = senv.fromElements(1, 2, 3, 4);


        elements.addSink(new RichSinkFunction<Integer>() {
            @Override
            public void invoke(Integer value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        senv.execute("add sink");

    }
}
