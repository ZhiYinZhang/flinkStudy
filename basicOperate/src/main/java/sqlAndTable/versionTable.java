package sqlAndTable;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class versionTable {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        String sql= "create table test1(" +
                "`id` int," +
                "`name` string," +
                "`age` int," +
                "`update_time` timestamp with local time zone,"+
                "`event_time` as to_timestamp(date_format(update_time,'yyyy-MM-dd HH:mm:ss'))," +
//                "`update_time` TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL," +
                "primary key(id) not enforced," +
                "watermark for event_time as event_time" +
                ")with(" +
                "'connector'='kafka'," +
                "'topic'='fullfillment.test.test1'," +
                "'properties.bootstrap.servers'='192.168.35.164:9092'," +
                "'properties.group.id'='testGroup'," +
                "'scan.startup.mode'='latest-offset'," +
                "'debezium-json.schema-include'='true',"+
                "'debezium-json.timestamp-format.standard'='ISO-8601',"+
                "'format'='debezium-json'" +
                ")";

        stenv.executeSql(sql);
        Table table = stenv.sqlQuery("select * from test1");
        DataStream<Tuple2<Boolean, Tuple>> tuple2DataStream = stenv.toRetractStream(table, Types.TUPLE(
                Types.INT,
                Types.STRING,
                Types.INT,
                Types.INSTANT,
                Types.SQL_TIMESTAMP
        ));
        tuple2DataStream.print();

        senv.execute();
    }
}
