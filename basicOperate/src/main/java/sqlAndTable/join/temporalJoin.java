package sqlAndTable.join;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class temporalJoin {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        String sql1= "create table orders(" +
                "`id` int," +
                "`order_id` string," +
                "`user_id` int," +
                "`create_time` timestamp(3)," +
                "watermark for create_time as create_time" +
                ")with(" +
                "'connector'='kafka'," +
                "'topic'='test'," +
                "'properties.bootstrap.servers'='192.168.35.164:9092'," +
                "'properties.group.id'='testGroup'," +
                "'scan.startup.mode'='latest-offset'," +
                "'format'='csv'" +
                ")";

        String sql2= "create table users(" +
                "`id` int," +
                "`name` string," +
                "`age` int," +
                "`update_time` timestamp with local time zone," +
                "`event_time` as to_timestamp(date_format(update_time,'yyyy-MM-dd HH:mm:ss'))," +
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

        stenv.executeSql(sql1);

//        Table table = stenv.sqlQuery("select * from orders");
//        DataStream<Tuple> tupleDataStream = stenv.toAppendStream(table, Types.TUPLE(
//                Types.INT,
//                Types.STRING,
//                Types.INT,
//                Types.SQL_TIMESTAMP
//        ));
//        tupleDataStream.print();

//        Table table = stenv.sqlQuery("select id,name,age,update_time,event_time from users");
//        DataStream<Tuple2<Boolean, Tuple>> tuple2DataStream = stenv.toRetractStream(table, Types.TUPLE(
//                Types.INT,
//                Types.STRING,
//                Types.INT,
//                Types.INSTANT,
//                Types.SQL_TIMESTAMP
//        ));
//        tuple2DataStream.print();


        stenv.executeSql(sql2);
        Table table = stenv.sqlQuery("select o.order_id,o.user_id,o.create_time,u.id,u.name,u.age,u.event_time \n" +
                "from orders o \n" +
                "left join users for system_time as of o.create_time as u \n" +
                "on o.user_id=u.id");


        DataStream<Tuple2<Boolean, Tuple>> tuple2DataStream = stenv.toRetractStream(table, Types.TUPLE(
                Types.STRING,
                Types.INT,
                Types.SQL_TIMESTAMP,

                Types.INT,
                Types.STRING,
                Types.INT,
                Types.SQL_TIMESTAMP
        ));
        tuple2DataStream.print();

        senv.execute();
    }
}
