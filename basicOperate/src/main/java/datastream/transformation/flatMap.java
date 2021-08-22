package datastream.transformation;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class flatMap {
    public static void main(String[] args) throws Exception{
        //init environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> lines = env.fromElements("spark flink hive hadoop yarn hdfs hive spark flink");

        //transformation
        //第一种：使用FlatMapFunction接口
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //使用java8的stream的forEach方法;out::collect的lambda表达式写法为(x->out.collect(x))
                Arrays.stream(value.split(" ")).forEach(out::collect);
            }
        });
        //第二种：使用lambda表达式
        //line为输入，out为输出,注意flatMap必须指定返回值得类型
        SingleOutputStreamOperator<String> words2 = lines.flatMap((String line,Collector<String> out) ->
                Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);

        //第三种：flatMap方法还可以传入RichFlatMapFunction
//        类似RichMapFunction



        //sink
//        words.print();
        words2.print();


        //execute
        env.execute();

    }
}
