package datastream.operators.asyncIO;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class AsynIO {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = senv.fromElements("1 2 3 4");

        DataStream<String> ids = stream.flatMap((String line, Collector<String> out) -> {
            String[] split = line.split(" ");
            Arrays.stream(split).forEach(out::collect);
        }).returns(Types.STRING);


        DataStream<String> resultStream =
                AsyncDataStream.unorderedWait(ids, new mysqlAsync(), 1000, TimeUnit.MILLISECONDS, 100);

        resultStream.print();
        senv.execute();
    }
}

