package sqlAndTable;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.joda.time.DateTime;


import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 时间属性，如果是datastream转table，需要分配时间戳和watermark
 * 然后在指定列的时候使用rowtime
 */
public class timeAttr1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //word,time;如spark,2021-08-10
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);
        DataStream<Tuple2<String, String>> map = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] split = s.split(",");
                return Tuple2.of(split[0], split[1]);
            }
        });

        WatermarkStrategy<Tuple2<String,String>> watermarkStrategy = WatermarkStrategy.<Tuple2<String,String>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,String>>() {
            @Override
            public long extractTimestamp(Tuple2<String,String> s, long l) {
                long millis = DateTime.parse(s.f1).getMillis();
                return millis;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, String>> tuple2SingleOutputStreamOperator = map.assignTimestampsAndWatermarks(watermarkStrategy);

        //使用rowtime()指定cTime为时间属性列
        tenv.createTemporaryView("myTable",tuple2SingleOutputStreamOperator,$("word"),$("cTime_str"),$("cTime").rowtime());


//        Table table = tenv.sqlQuery("select word,cTime_str,cTime from myTable");
//        DataStream<Tuple2<Boolean, Tuple>> tuple2DataStream = tenv.toRetractStream(table, Types.TUPLE(Types.STRING,Types.STRING,Types.SQL_TIMESTAMP));
//        System.out.println(table.getSchema());


        Table table = tenv.sqlQuery("select word,count(1) c,TUMBLE_START(cTime, INTERVAL '5' day) as wStart from myTable group by tumble(cTime,interval '5' day),word");
        table.printSchema();

        DataStream<Tuple2<Boolean, Tuple>> tuple2DataStream = tenv.toRetractStream(table, Types.TUPLE(Types.STRING, Types.LONG, Types.SQL_TIMESTAMP));
        tuple2DataStream.print();

        env.execute();

    }
}
