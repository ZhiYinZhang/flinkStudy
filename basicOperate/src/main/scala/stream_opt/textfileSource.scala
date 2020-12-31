package stream_opt

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object textfileSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /*
    这个source虽然是流执行环境里面的api，但是计算完就会退出
     */
    env.readTextFile("e://data//flink.txt")
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()

    /*
    这个流会隔一段时间检查文件是否更改;更改，就会全量读取文件里面的数据,不会只读取新增的数据
     */
    env.readFile(
      new TextInputFormat(null),
      "file:///e://data//flink.txt",
      FileProcessingMode.PROCESS_CONTINUOUSLY,
      3000
    )
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()


    env.execute()
  }
}
