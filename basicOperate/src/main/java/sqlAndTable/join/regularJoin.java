package sqlAndTable.join;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 这个是最灵活的，允许任何类型的更新(插入、更新、删除)输入表
 */
public class regularJoin {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        String kafkaSql1= "create table test1(" +
                "`id` int," +
                "`name` string," +
                "`age` int," +
                "`update_time` timestamp" +
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
        String kafkaSql2= "create table test2(" +
                "`id` int," +
                "`name` string," +
                "`age` int," +
                "`update_time` timestamp" +
                ")with(" +
                "'connector'='kafka'," +
                "'topic'='fullfillment.test.test2'," +
                "'properties.bootstrap.servers'='192.168.35.164:9092'," +
                "'properties.group.id'='testGroup'," +
                "'scan.startup.mode'='latest-offset'," +
                "'debezium-json.schema-include'='true',"+
                "'debezium-json.timestamp-format.standard'='ISO-8601',"+
                "'format'='debezium-json'" +
                ")";

        stenv.executeSql(kafkaSql1);
        stenv.executeSql(kafkaSql2);
        Table table = stenv.sqlQuery("select a.id,a.name,a.age,b.id,b.name,b.age " +
                "from test1 as a " +
                "join test2 as b " +
                "on a.name = b.name");

        DataStream<Tuple2<Boolean, Tuple>> tuple2DataStream = stenv.toRetractStream(table, Types.TUPLE(
                Types.INT,
                Types.STRING,
                Types.INT,
                Types.INT,
                Types.STRING,
                Types.INT
        ));

        tuple2DataStream
                .filter(new FilterFunction<Tuple2<Boolean, Tuple>>() {
            @Override
            public boolean filter(Tuple2<Boolean, Tuple> booleanTupleTuple2) throws Exception {
                return booleanTupleTuple2.f0;
            }
        })
            .print();

        senv.execute();

        /**
         * 报错：IntervalJoin doesn't support consuming update and delete changes which is produced by node TableSourceScan
         * http://apache-flink.147419.n8.nabble.com/Flink-SQL-Interval-Join-watermark-td11637.html
         */
    }
}
