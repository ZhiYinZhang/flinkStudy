import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class transformation {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4);

        SingleOutputStreamOperator<Integer> res = nums.map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer integer) throws Exception {
                return integer * 2;
            }
        });

        SingleOutputStreamOperator<Integer> res2 = nums.map(i -> i * 2).returns(Types.INT);

        res.print();
        res2.print();

        env.execute("transformation");
    }
}
