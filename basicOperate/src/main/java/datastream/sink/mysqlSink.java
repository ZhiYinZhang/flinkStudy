package datastream.sink;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class mysqlSink {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        Tuple4<Integer, String, String, Integer> data = Tuple4.of(6, "test", "math", 90);

        DataStreamSource<Tuple4<Integer, String, String, Integer>> elements = senv.fromElements(data);

        elements.addSink(JdbcSink.sink(
                "insert into temp values(?,?,?,?)",//sql语句
                //设置sql中占位符对应的字段值
                (ps,tp)->{ //tp是DataStream里面的数据，Tuple4
                    ps.setInt(1,tp.f0);
                    ps.setString(2, tp.f1);
                    ps.setString(3,tp.f2);
                    ps.setDouble(4,tp.f3);
                },
                //jdbc连接配置
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUrl("jdbc:mysql://localhost:3306/test?serverTimezone=Asia/Shanghai")
                .withUsername("root")
                .withPassword("123456")
                .build()
        ));

        elements.print();
        senv.execute();
    }
}
