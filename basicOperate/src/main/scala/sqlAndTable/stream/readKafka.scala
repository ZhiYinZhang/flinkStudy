package sqlAndTable.stream

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableResult}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object readKafka {
  def main(args: Array[String]): Unit = {
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stenv: StreamTableEnvironment = StreamTableEnvironment.create(senv)

    import org.apache.flink.api.scala._


    val kafkaSql="""
      |create table kafkaTable(
      |id int,
      |name string,
      |age int
      |)with(
      |'connector'='kafka',
      |'topic'='KAFKATEST02',
      |'properties.bootstrap.servers'='node2.hadoop.com:9092',
      |'properties.group.id'='testGroup',
      |'scan.startup.mode'='latest-offset',
      |'format'='csv',
      |'value.format'='csv'
      |)
      |""".stripMargin


    stenv.executeSql(kafkaSql)

    val table: Table = stenv.sqlQuery("select * from kafkaTable")


    //输出需要转成datastream
    val value: DataStream[(Int,String,Int)] = stenv.toAppendStream(table)
    value.print()


    senv.execute()

  }
}
