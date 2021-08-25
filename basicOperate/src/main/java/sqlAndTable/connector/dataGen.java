package sqlAndTable.connector;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class dataGen {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        String sql = "CREATE TABLE Orders ("+
                "order_number BIGINT,"+
                "price        DECIMAL(32,2),"+
                "buyer        ROW<first_name STRING, last_name STRING>,"+
                "order_time   TIMESTAMP(3)"+
        ") WITH ("+
                "'connector' = 'datagen'," +
                "'fields.order_number.kind' = 'random'," +
                "'fields.order_number.min' = '0'," +
                "'fields.order_number.max' = '10'," +
                "'fields.buyer.first_name.length' = '5'," +
                "'fields.buyer.last_name.length' = '5'," +
                "'fields.price.kind' = 'random',"+
                "'rows-per-second' = '1'"+
        ")";

        stenv.executeSql(sql);
        Table table = stenv.sqlQuery("select * from Orders");
        DataStream<Tuple> tupleDataStream = stenv.toAppendStream(table, Types.TUPLE(
                Types.LONG,
                Types.BIG_DEC,
                Types.ROW(Types.STRING, Types.STRING),
                Types.SQL_TIMESTAMP
        ));
        tupleDataStream.print();





        senv.execute();
    }
}
