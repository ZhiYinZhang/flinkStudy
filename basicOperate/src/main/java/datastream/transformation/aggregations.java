package datastream.transformation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/*
这里以max为例，求组内value最大的数据
 */
public class aggregations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
        数据格式：
        a,10
        a,5
        a,11
         */
        DataStreamSource<String> lines = env.socketTextStream("cdh3", 9999);

        SingleOutputStreamOperator<Tuple2<String, Double>> wordAndValue = lines.map(value -> {
            String[] split = value.split(",");
            return Tuple2.of(split[0], Double.parseDouble(split[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.DOUBLE));

        KeyedStream<Tuple2<String, Double>, Tuple> keyed = wordAndValue.keyBy(0);



        //每输入一个值就比较一次，然后输出最大的值
       keyed.max(1).print();

        env.execute();
    }
}
