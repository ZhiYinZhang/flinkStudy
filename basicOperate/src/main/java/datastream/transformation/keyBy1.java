package datastream.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * keyBy分别使用flink的Tuple封装数据，使用java bean封装数据
 */
public class keyBy1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //这里直接输入一行一个单词
        DataStreamSource<String> words = env.socketTextStream("localhost", 9999);


        //keyBy
        //第一种：使用flink java里面的元组封装数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(x -> Tuple2.of(x, 1)).returns(Types.TUPLE(Types.STRING,Types.INT));
        // KeyedStream<Tuple2<String, Integer>, String>第一个为数据，第二个为key
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(value -> value.f0);



        //第二种：使用java bean的方式封装数据
        SingleOutputStreamOperator<wordCounts> wordAndOne2 = words.map(new MapFunction<String, wordCounts>() {
            @Override
            public wordCounts map(String value) throws Exception {
                return wordCounts.of(value, 1L);
            }
        });
        KeyedStream<wordCounts, String> keyed2 = wordAndOne2.keyBy(value -> value.word);


//        keyed.print();
//        keyed2.print();


        //聚合
//        keyed.sum(1).print();
        keyed2.sum("counts").print();

        env.execute();
    }
}
