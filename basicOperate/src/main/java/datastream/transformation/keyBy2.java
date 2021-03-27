package datastream.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用多个字段分组
 */
public class keyBy2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 数据格式：
         *省级,市级,金额
         * 广东,广州,1000
         * 广东,深圳,1500
         * 广西,南宁,1100
         * 广西,桂林,1000
         */

        DataStreamSource<String> lines = env.socketTextStream("cdh3", 9999);


        //keyBy
        SingleOutputStreamOperator<Tuple3<String, String, Double>> map = lines.map(line -> {
            String[] splits = line.split(",");
            return Tuple3.of(splits[0], splits[1], Double.parseDouble(splits[2]));
        }).returns(Types.TUPLE(Types.STRING,Types.STRING,Types.DOUBLE));

        KeyedStream<Tuple3<String, String, Double>, Tuple> keyBy = map.keyBy(0, 1);

        keyBy.sum(2).print();
//        keyBy.print();



        //聚合
//        keyed.sum(1).print();


        env.execute();
    }
}
