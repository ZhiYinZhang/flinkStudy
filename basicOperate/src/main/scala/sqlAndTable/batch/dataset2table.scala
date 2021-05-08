package sqlAndTable.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
//类似spark的functions
import org.apache.flink.table.api.{AnyWithOperations, FieldExpression, Table,string2Literal}
//隐士转换
import org.apache.flink.api.scala._
object dataset2table {
	def main (args: Array[String]): Unit = {
		val benv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
		val btenv: BatchTableEnvironment = BatchTableEnvironment.create(benv)

		val name: DataSet[(Int, String)] = benv.fromElements((1, "jerry"), (2, "jack"), (3, "don"))

		//dataset转table
		val table: Table = btenv.fromDataSet(name,$"id",$"name")

		table.printSchema()

		val where: Table = table.where($"name" === "jack")

    //打印需要转成dataset
		val value: DataSet[(Int,String)] = btenv.toDataSet(where)

		value.print()


	}
}
