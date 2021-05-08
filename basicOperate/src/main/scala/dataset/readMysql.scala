package dataset

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.connector.jdbc.{JdbcInputFormat, JdbcOutputFormat}
import org.apache.flink.types.Row


object readMysql {
  def main(args: Array[String]): Unit = {
    val benv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._


    val format: JdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://node6.hadoop.com:3306/test")
      .setUsername("root")
      .setPassword("CJYcjy!@#$%^123456")
      .setQuery("select id,name,age from flink_test")
      .setRowTypeInfo(new RowTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO
      ))
      .finish()

    val result: DataSet[Row] = benv.createInput(format)

    result.print()

  }
}
