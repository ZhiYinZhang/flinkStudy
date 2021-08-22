package datastream

import java.lang
import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.NumberSequenceIterator
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
object demo2 {
  def main(args: Array[String]): Unit = {
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

     senv.readFile(new TextInputFormat(null),
       "file:///e://data//flink.txt",
       FileProcessingMode.PROCESS_CONTINUOUSLY,
       3000
     )
       .flatMap(_.split(" "))
       .map((_,1))
       .keyBy(0)
       .reduce((r1,r2)=>{
         (r1._1,r1._2+r2._2)
    })
       .print()

    senv.execute()
  }
}
