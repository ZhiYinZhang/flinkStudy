import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object first {
	def main (args: Array[String]): Unit = {
				val envs: StreamExecutionEnvironment = StreamExecutionEnvironment
				.getExecutionEnvironment

		val conf=new Configuration()
		conf.setString("rest.address","127.0.0.1")
		conf.setInteger("rest.port",8081)

		val text: DataStream[String] = envs.readTextFile("E:\\test\\flinkData\\demo1\\a.txt")
		println("parallelism:", text.parallelism)
		text.print()

		envs.execute("first flink job")

		Thread.sleep(30*1000)
	}
}
