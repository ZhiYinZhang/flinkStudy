package datastream.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class filter {
    public static void main(String[] args) throws Exception {
        //init environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<Integer> nums = env.fromElements(1,2,3,4,5,6,7,8,9);

        //transformation
        //第一种：使用FilterFunction
        SingleOutputStreamOperator<Integer> even = nums.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 2 == 0;
            }
        });


        //第二种：使用lambda表达式
        SingleOutputStreamOperator<Integer> even2 = nums.filter(num -> num %2==0);


        //sink
        even.print();
        even.print();

        //execute
        env.execute();
    }
}
