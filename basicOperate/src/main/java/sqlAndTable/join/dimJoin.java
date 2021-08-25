package sqlAndTable.join;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class dimJoin {
    public static void main(String[] args) throws Exception {
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

        String sql2 = "create table users( \n" +
                "`id` int,\n" +
                "`name` string,\n" +
                "`age` int,\n" +
                "`update_time` timestamp(3)\n" +
                ")with(\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:mysql://192.168.35.166:3306/test',\n" +
                "'table-name' = 'test1',\n" +
                "'username' = 'root',\n" +
                "'password' = 'CJYcjy!@#$%^123456',\n" +
                "'lookup.cache.max-rows' = '100',\n" +
                "'lookup.cache.ttl' = '10s'" +
                ")";

        stenv.executeSql(sql1);
        stenv.executeSql(sql2);

//        Table table = stenv.sqlQuery("select id,name,age,update_time from users");
//        DataStream<Tuple2<Boolean, Tuple>> tuple2DataStream = stenv.toRetractStream(table, Types.TUPLE(
//                Types.INT,
//                Types.STRING,
//                Types.INT,
//                Types.SQL_TIMESTAMP
//        ));
//        tuple2DataStream.print();

                Table table = stenv.sqlQuery("select o.user_id,u.id,u.name,u.age from orders o join users  u on" +
                        " o.user_id = u.id");
        DataStream<Tuple2<Boolean, Tuple>> tuple2DataStream = stenv.toRetractStream(table, Types.TUPLE(
                Types.INT,
                Types.INT,
                Types.STRING,
                Types.INT
        ));
        tuple2DataStream.print();


        senv.execute();
    }
}
