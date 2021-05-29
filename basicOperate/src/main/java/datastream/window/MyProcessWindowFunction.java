package datastream.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;


public class MyProcessWindowFunction extends ProcessAllWindowFunction<Integer, Tuple2<String,Integer>, TimeWindow> {

    @Override
    public void process(Context context, Iterable<Integer> elements, Collector<Tuple2<String,Integer>> out) throws Exception {
        Integer sum=0;

        for (Integer element:elements) {
             sum+=element;
        }

        TimeWindow window = context.window();
        long start = window.getStart();
        long end = window.getEnd();
        String start_timestamp = new Timestamp(start).toString();
        String end_timestamp = new Timestamp(end).toString();

        out.collect(Tuple2.of("window: "+start_timestamp+" ~ "+end_timestamp,sum));

    }
}
