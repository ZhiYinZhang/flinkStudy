package sqlAndTable.join;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 与regular join相比，这种连接只支持带有时间属性的仅追加表。由于时间属性是准单调递增的，
 * Flink可以在不影响结果正确性的情况下从其状态中删除旧值
 */
public class intervalJoin {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        //下单表
        String sql1 = "create table orders(" +
                "id int," +
                "order_time timestamp(3)" +
                ")with(" +
                "'connector' = 'datagen'," +
                "'rows-per-second' = '2'," +   //下单生成速率要快一点
                "'fields.id.kind' = 'sequence'," +
                "'fields.id.start' = '1'," +
                "'fields.id.end' = '1000'" +
                ")";
        //发货表
        String sql2 = "create table shipments(" +
                "id int," +
                "order_id int," +
                "shipment_time timestamp(3)" +
                ")with(" +
                "'connector' = 'datagen'," +
                "'rows-per-second' = '1'," +
                "'fields.id.kind' = 'random'," +
                "'fields.id.min' = '0'," +
                "'fields.order_id.kind' = 'sequence'," +
                "'fields.order_id.start' = '1'," +
                "'fields.order_id.end' = '1000'" +
                ")";

        stenv.executeSql(sql1);
        stenv.executeSql(sql2);
        Table table = stenv.sqlQuery("select o.id,o.order_time,s.shipment_time from orders o \n" +
                "join shipments s \n" +
                "on o.id = s.order_id \n" +
                "where o.order_time between s.shipment_time - interval '3' second and s.shipment_time");

        stenv.toRetractStream(table, Types.TUPLE(
                Types.INT,
                Types.SQL_TIMESTAMP,
                Types.SQL_TIMESTAMP
        )).print();

        senv.execute();
    }
}
