package com.machinedoll.experiment

import com.machinedoll.experiment.data.TestData
import com.machinedoll.experiment.sink.TestDataKafkaAvroSink
import com.machinedoll.experiment.source.SlowEmitSource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.kafka.common.protocol.types.Schema

object Producer {

  var schemaVersion: Schema = _
  var sleepInterval: Long = _

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val testStream: DataStream[TestData] = env
      .addSource(new SlowEmitSource())

    testStream.addSink(TestDataKafkaAvroSink.simpleStringProducer)

    env.execute("Demo Consumer: Load Schema From External Schema Register and Send to Kafka")
  }

}
