package datastream.sink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


public class csvSink {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<Long> nums = senv.fromSequence(1, 10);

        DataStreamSource<String> lines = senv.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<String> words = lines.flatMap((String line, Collector<String> out) ->
                Arrays.stream(line.split(",")).forEach(out::collect)
        ).returns(Types.STRING);



        words.map(x-> Tuple1.of(x)).returns(Types.TUPLE(Types.STRING))
                .writeAsCsv("e://test//flinkData//csv", FileSystem.WriteMode.OVERWRITE);

        words.startNewChain();
        words.disableChaining();

        senv.execute();
    }
}
