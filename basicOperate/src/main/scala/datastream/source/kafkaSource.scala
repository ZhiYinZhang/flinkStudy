package datastream.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object kafkaSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.35.162:9092")
    properties.setProperty("group.id", "test1")

    val kafkaSource: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](
      "KAFKATEST02", //需要消费的topic
      new SimpleStringSchema(), //topic数据反序列化成string
      properties //连接参数
    )
    //从最开始消费
//    kafkaSource.setStartFromEarliest()

    val lines: DataStream[String] = env.addSource(kafkaSource)

    lines.print()

//    lines.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1).print()

    env.execute("kafka source")

  }
}
