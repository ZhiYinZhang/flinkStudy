package stream_opt

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

//隐士转换
import org.apache.flink.api.scala._
object wordCount {
 def main(args:Array[String]):Unit={
   //1.创建flink stream程序的执行环境
   val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


   //2.source,读取数据，创建DataStream
   val lines: DataStream[String] = env.socketTextStream("cdh3", 9999)


   //3.transformation(可选)
   val words: DataStream[String] = lines.flatMap(_.split(" "))

   val wordAndOne: DataStream[(String, Int)] = words.map((_, 1))

   val summed: DataStream[(String, Int)] = wordAndOne.keyBy(_._1).sum(1)


   //4.sink
   //输出到控制台
    summed.print()

   //5.执行
   env.execute(this.getClass.getSimpleName)
 }
}
