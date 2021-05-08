package sqlAndTable.batch

import java.sql.Timestamp

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.table.factories.TableSourceFactory
import org.apache.flink.types.Row
object sql2Table {
  def main(args: Array[String]): Unit = {
    val benv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val btenv: BatchTableEnvironment = BatchTableEnvironment.create(benv)
    import org.apache.flink.api.scala._


    val createTable="""
      |create table flink_test1(
      |id int,
      |name string,
      |age int
      |) with (
      |'connector.type' = 'jdbc',
      |'connector.url' = 'jdbc:mysql://192.168.35.166:3306/test',
      |'connector.driver' = 'com.mysql.jdbc.Driver',
      |'connector.username' = 'root',
      |'connector.password' = 'CJYcjy!@#$%^123456',
      |'connector.table' = 'flink_test'
      |)
      |""".stripMargin
    val printSql="""
      |create table sink_print(
      |id int,
      |name string,
      |age int
      |)with(
      |'connector'='print'
      |)
      |""".stripMargin

    btenv.executeSql(createTable)
    btenv.executeSql(printSql)



//    btenv.execute("create table test_kafka(id int,`count` int,`timestamp` timestamp)with('connector'='jdbc','url'='jdbc:mysql://192.168.35.166:3306/kafka','driver'='com.mysql.jdbc.Driver','username'='root','password'='CJYcjy!@#$%^123456','table-name'='test_kafka')")




    val table: Table = btenv.sqlQuery("select * from flink_test1")

    table.printSchema()


    val value: DataSet[(Int,String,Int)] = btenv.toDataSet[(Int,String,Int)](table)
    value.print()


//    btenv.toDataSet[Row](table).print()
  }
}
