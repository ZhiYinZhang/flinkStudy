package datastream.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class MyAggregateWindowFunction {
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

        SingleOutputStreamOperator<Double> process = map
                .keyBy(r -> r.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<Long,Long>, Double>() {

                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                        return Tuple2.of(0L,0L);
                    }

                    @Override
                    public Tuple2<Long, Long> add(Tuple2<String, Integer> value, Tuple2<Long, Long> accumulator) {
                        return Tuple2.of(accumulator.f1+value.f1,accumulator.f0+1);
                    }

                    @Override
                    public Double getResult(Tuple2<Long, Long> accumulator) {
                        return ((double) accumulator.f0)/accumulator.f1;
                    }

                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                        return Tuple2.of(a.f0+b.f0,a.f1+b.f1);
                    }
                });


        process.print();
        senv.execute();
    }
}
