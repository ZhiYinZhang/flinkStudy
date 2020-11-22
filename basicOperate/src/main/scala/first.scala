import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//引入隐式转换
import org.apache.flink.api.scala._

object first {
	def main (args: Array[String]): Unit = {
		val envs: StreamExecutionEnvironment = StreamExecutionEnvironment
		.getExecutionEnvironment



		val text: DataStream[String] = envs.readTextFile("E:\\test\\flinkData\\demo1\\a.txt")

		val value = text.map(x => x + "aaaaaaaaaaa")

		println("parallelism:", value.parallelism)
		value.print()

		envs.execute("first flink job")

		Thread.sleep(30*1000)
	}
}
