package com.machinedoll.experiment

import java.util.Properties

import com.machinedoll.experiment.data.TestData
import com.machinedoll.experiment.source.SlowEmitSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object Producer {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "10.8.3.102:6667")

    val simpleStringProducer =
      new FlinkKafkaProducer[String]("test", new SimpleStringSchema(), properties)

    val testStream: DataStream[TestData] = env
      .addSource(new SlowEmitSource())

    testStream.print()

    testStream
      .map(_.toString)
      .addSink(simpleStringProducer)

    env.execute("Demo Consumer: Load Schema From External Schema Register and Send to Kafka")
  }

}
