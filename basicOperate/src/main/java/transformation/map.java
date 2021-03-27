package transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class map {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4);

        //第一种：使用MapFunction接口
        SingleOutputStreamOperator<Integer> res = nums.map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer integer) throws Exception {
                return integer * 2;
            }
        });

        //第二种：lanbda表达式，有的操作需要指定returns返回值类型，最好指定
        SingleOutputStreamOperator<Integer> res2 = nums.map(i -> i * 2).returns(Types.INT);

        //第三种：RichMapFunction接口，有更丰富的方法
        SingleOutputStreamOperator<Integer> rich_res = nums.map(new RichMapFunction<Integer, Integer>() {
            //open，在构造方法之后，map方法执行之前，每个subTask执行一次，Configuration可以拿到全局配置
            //用来初始化一些链接，或初始化或恢复state
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("初始化");
            }

            @Override
            public Integer map(Integer integer) throws Exception {
                return integer * 2;
            }

            //每个subTask销毁之前，执行一次，通常是作为资源释放
            @Override
            public void close() throws Exception {
                System.out.println("关闭");
            }
        });

//        res.print();
//        res2.print();
        rich_res.print();

        env.execute("transformation");
    }
}
