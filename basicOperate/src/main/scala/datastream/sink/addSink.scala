package datastream.sink

import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * 自定义sink，这里只是演示一下，实现print
 */
object addSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val lines: DataStream[String] = env.socketTextStream("cdh3", 9999)


    lines.addSink(new RichSinkFunction[String] {
      override def invoke(value: String, context: SinkFunction.Context): Unit = {
        //获取subTask的索引编号
        val index: Int = getRuntimeContext.getIndexOfThisSubtask

        println(s"########## $index>$value ##########")

      }
    })


    env.execute()
  }
}
