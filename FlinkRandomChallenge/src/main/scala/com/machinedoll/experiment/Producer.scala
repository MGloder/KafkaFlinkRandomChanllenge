package com.machinedoll.experiment

import com.machinedoll.experiment.data.TestData
import com.machinedoll.experiment.processor.TestDataKafkaAvroSink
import com.machinedoll.experiment.source.SlowEmitSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.kafka.common.protocol.types.Schema

object Producer {

  var schemaVersion: Schema = _
  var sleepInterval: Long = _

  //  def requestSchema(getSimpleName: String, version: String): Schema = {
  //    val requestString = getSimpleName.toLowerCase + "-avro-" + version
  //    print(requestString)
  //
  //  }

  //  def prepareRuntimeEnvironment() = {
  //    schemaVersion = requestSchema(TestData.getClass.getSimpleName, "v1")
  //    sleepInterval = 1000
  //  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    env
      .addSource(new SlowEmitSource(sleepInterval))
      .addSink(TestDataKafkaAvroSink.getSimpleString("simple-string-topic"))

    //    testDataStream
    //      .map(new ConvertPOJOToString)
    //      .addSink(TestDataKafkaAvroSink.getSimpleString("simple-string-topic"))
    env.execute("Demo Consumer: Load Schema From External Schema Register and Send to Kafka")
  }

}
