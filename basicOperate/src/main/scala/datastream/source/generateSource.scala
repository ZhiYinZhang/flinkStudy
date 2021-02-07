package datastream.source

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.NumberSequenceIterator

object generateSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    //单并行度的source
    val collSource: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5))
    val eleSource: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 7, 8, 9, 10)
    val socketSource: DataStream[String] = env.socketTextStream("cdh3", 9999)
    //多并行度的source
    val seqSource: DataStream[Long] = env.fromSequence(1, 100)
    val paraCollSource = env.fromParallelCollection(new NumberSequenceIterator(0, 100))

    val filter = collSource.filter(_ % 2 != 0)


    println(s"collection source的并行度:${collSource.parallelism}")
    println(s"element source的并行度:${eleSource.parallelism}")
    println(s"socket的并行度:${socketSource.parallelism}")
    println(s"转换后的并行度:${filter.parallelism}")


    println(s"sequence source的并行度:${seqSource.parallelism}")
    println(s"parallel collection source的并行度:${paraCollSource.parallelism}")


    env.readTextFile("e://data//flink.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print()

    env.readFile(
      new TextInputFormat(null),
      "file:///e://data//flink.txt",
      FileProcessingMode.PROCESS_CONTINUOUSLY,
      3000
    )
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print()



    //    filter.print()

    env.execute()


  }
}
