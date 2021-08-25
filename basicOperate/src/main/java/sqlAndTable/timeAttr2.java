package sqlAndTable;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 在DDL是定义时间属性
 */
public class timeAttr2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        String sql = "CREATE TABLE Orders (\n"+
                "order_number BIGINT,\n"+
                "price        DECIMAL(32,2),\n"+
                "buyer        ROW<first_name STRING, last_name STRING>,\n"+
                "order_time   TIMESTAMP(3),\n"+
                "WATERMARK for order_time as order_time - interval '0' second" +
        ") WITH ("+
                "'connector' = 'datagen'"+
//                "'number-of-'"+
        ")";

        stenv.executeSql(sql);
        Table table = stenv.sqlQuery("select tumble_start(order_time,interval '5' second) wStart," +
                "tumble_end(order_time,interval '5' second) wEnd," +
                "count(1) as c " +
                "from Orders group by tumble(order_time,interval '5' second)");

        DataStream<Tuple> tupleDataStream = stenv.toAppendStream(table, Types.TUPLE(
                Types.SQL_TIMESTAMP,
                Types.SQL_TIMESTAMP,
                Types.LONG
        ));
        tupleDataStream.print();

        senv.execute();
    }
}
