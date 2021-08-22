package sqlAndTable;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class readKafkaJson {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        String kafkaSql= "create table kafkaTable(" +
                "`id` int," +
                "`name` string," +
                "`age` int," +
                "`partition` int metadata virtual," +
                "`offset` int metadata virtual," +
                "`event_time` timestamp(3) metadata from 'timestamp'"+
                ")with(" +
                "'connector'='kafka'," +
                "'topic'='test'," +
                "'properties.bootstrap.servers'='192.168.35.164:9092'," +
                "'properties.group.id'='testGroup'," +
                "'scan.startup.mode'='latest-offset'," +
                "'format'='json'," +
                "'value.format'='json'" +
                ")";

        stenv.executeSql(kafkaSql);
        Table table = stenv.sqlQuery("select * from kafkaTable");

        DataStream<Tuple> tupleDataStream = stenv.toAppendStream(table, Types.TUPLE(Types.INT, Types.STRING, Types.INT,Types.INT,Types.INT,Types.SQL_TIMESTAMP));
        tupleDataStream.print();

        senv.execute();
    }
}
