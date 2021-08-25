package sqlAndTable;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class readSocket {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                Arrays.stream(line.split(" ")).forEach(out::collect);
            }
        });

        //将datastream转成Table，并将datastream第一个字段重命名为word
//        Table word = tenv.fromDataStream(words, $("word"));
//        Table word1 = word.groupBy("word").select("word,count(1) as c");


        //将datastream转成sql table,并将datastream第一个字段重命名为word
//        tenv.createTemporaryView("myTable",words,"word");
        //将datastream转成sql table,使用Expression并将datastream第一个字段重命名为word
        tenv.createTemporaryView("myTable",words,$("word"));

        Table table = tenv.sqlQuery("select word,count(1) as c from myTable group by word");

        tenv.executeSql("show catalogs").print();
        tenv.executeSql("show databases").print();

        //将table转成datastream
        DataStream<Tuple2<Boolean, Tuple>> tuple2DataStream = tenv.toRetractStream(table, Types.TUPLE(Types.STRING, Types.LONG));
        tuple2DataStream.print();

        env.execute();
    }
}
