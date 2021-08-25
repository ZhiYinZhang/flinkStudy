package sqlAndTable.connector;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class debeziumCDCMysql {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        String kafkaSql1= "create table kafkaTable1(" +
                "`id` int," +
                "`count` int," +
                "`timestamp` timestamp with local time zone" +
                ")with(" +
                "'connector'='kafka'," +
                "'topic'='fullfillment.kafka.test_kafka'," +
                "'properties.bootstrap.servers'='192.168.35.164:9092'," +
                "'properties.group.id'='testGroup'," +
                "'scan.startup.mode'='latest-offset'," +
                "'debezium-json.schema-include'='true',"+
                "'debezium-json.timestamp-format.standard'='ISO-8601',"+
                "'format'='debezium-json'" +
                ")";
        String kafkaSql2= "create table kafkaTable2(" +
                "`id` int," +
                "`count` int," +
                "`timestamp` timestamp with local time zone" +
                ")with(" +
                "'connector'='kafka'," +
                "'topic'='fullfillment.kafka.test_kafka'," +
                "'properties.bootstrap.servers'='192.168.35.164:9092'," +
                "'properties.group.id'='testGroup'," +
                "'scan.startup.mode'='latest-offset'," +
                "'debezium-json.schema-include'='true',"+
                "'debezium-json.timestamp-format.standard'='ISO-8601',"+
                "'format'='debezium-json'" +
                ")";

        stenv.executeSql(kafkaSql1);
        stenv.executeSql(kafkaSql2);
        Table table = stenv.sqlQuery("select a.id,a.`count`,a.`timestamp` from kafkaTable1 as a join kafkaTable2 as b on a.id = b.id");

        DataStream<Tuple2<Boolean, Tuple>> tupleDataStream = stenv.toRetractStream(table, Types.TUPLE(Types.INT, Types.INT,Types.INSTANT));
//        DataStream<Tuple2<Boolean, Tuple>> tupleDataStream = stenv.toRetractStream(table, Types.TUPLE(Types.INT, Types.INT, Types.INSTANT, Types.INT, Types.INT, Types.INSTANT));

        tupleDataStream.print();

        senv.execute();
    }
}
