package datastream.sink


import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import org.apache.flink.streaming.api.scala._
/*
有的sink不会马上写数据目标数据源中
对目标系统的数据刷新取决于OutputFormat的实现。
这意味着并非所有发送到OutputFormat的元素都会立即显示在目标系统中。
同样，在失败的情况下，这些记录可能会丢失。
 */
object writeFile {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val lines: DataStream[String] = env.socketTextStream("cdh3", 9999)


    //1.写text file，这个会立即写入文件
//    lines.writeAsText("e://data//flink//sink//text")
//    lines.writeAsText("e://data//flink//sink//text",FileSystem.WriteMode.OVERWRITE)



    //2.写csv file
    /*
    csv只用用于Tuple datasets
    而且数据要达到4096字节才会刷入文件
     */

//    val value1: DataStream[(String, String)] = lines.map { value =>
//      val strings: Array[String] = value.split(" ")
//      (strings(0), strings(1))
//    }
//    value1.writeAsCsv("e://data//flink//sink//csv")
//    value1.writeAsCsv("e://data//flink//sink//csv",FileSystem.WriteMode.OVERWRITE)

    env.execute()
  }
}
