package datastream.transformation;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> words = env.socketTextStream("cdh3", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(x -> Tuple2.of(x, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);

        //使用reduce实现sum功能
        //第一种：使用ReduceFunction
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyed.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            //reduce的第一个参数为上次的结果(状态)，第二个参数为新的值
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                System.out.println(value1.f1 +"————"+value2.f1);
                String key = value1.f0;
                Integer count1 = value1.f1;
                Integer count2 = value2.f1;

                Integer count = count1 + count2;
                return Tuple2.of(key, count);
            }
        });

        //第二种：使用lambda表达式
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce1 = keyed.reduce((value1, value2) -> {
            System.out.println(value1.f1 +"————"+value2.f1);
            value1.f1 = value1.f1 + value2.f1;
            return value1;
        });

//        reduce.print();
        reduce1.print();

        env.execute();

    }
}
