package sqlAndTable.connector;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class readMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        stenv.executeSql("create table test1(" +
                "id int," +
                "name string," +
                "age int," +
                "update_time timestamp," +
                "primary key(id) not enforced" +
                ")with(" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:mysql://localhost:3306/test?serverTimeZone=Asia/ShangHai'," +
                "'table-name'='test1'," +
                "'username' = 'root'," +
                "'password' = '123456'" +
                ")");

        Table table = stenv.sqlQuery("select * from test1");
        DataStream<Tuple> tupleDataStream = stenv.toAppendStream(table, Types.TUPLE(
                Types.INT,
                Types.STRING,
                Types.INT,
                Types.SQL_TIMESTAMP
        ));
        tupleDataStream.print();

        senv.execute();
    }
}
