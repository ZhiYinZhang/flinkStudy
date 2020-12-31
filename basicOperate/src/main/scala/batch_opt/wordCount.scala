package batch_opt

import org.apache.flink.api.scala._

object wordCount {
def main(args:Array[String]):Unit={
  //1.创建flink batch程序的执行环境
  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


  //2.source,读取数据，创建DataSet
  val filePath="e://data//logData//app.csv"
  val lines: DataSet[String] = env.readTextFile(filePath)

  //3.transformation(可选)
  val words: DataSet[String] = lines.flatMap(_.split(","))

  val wordAndOne: DataSet[(String, Int)] = words.map((_, 1))

  //批处理中分组是groupBy函数，流式中是keyBy
  val res: AggregateDataSet[(String, Int)] = wordAndOne.groupBy(0).sum(1)


  //4.sink
  res.print()
//  res.setParallelism(3).writeAsCsv("e://data//flink_batch")

  //5.执行
  env.execute()

}
}
