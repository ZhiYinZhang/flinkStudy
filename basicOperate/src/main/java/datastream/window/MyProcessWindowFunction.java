package datastream.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

//参数：IN,OUT,KEY,W
public class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String,Integer>,Tuple3<String,String,Integer>,String,TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple3<String, String, Integer>> out) throws Exception {
        TimeWindow window = context.window();
        String start = new Timestamp(window.getStart()).toString();
        String end = new Timestamp(window.getEnd()).toString();

        long l = context.currentProcessingTime();
        System.out.println(new Timestamp(l).toString());

        Integer value=0;
        for (Tuple2<String, Integer> element:elements) {
            value+=element.f1;
        }

        out.collect(Tuple3.of(start+"~"+end,s,value));
    }
}
