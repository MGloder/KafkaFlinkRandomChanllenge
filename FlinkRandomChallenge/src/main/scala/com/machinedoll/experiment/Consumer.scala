package com.machinedoll.experiment

import com.machinedoll.experiment.data.TestData
import com.machinedoll.experiment.source.SlowEmitSource
import org.apache.avro.reflect.ReflectData
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Consumer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val schema = ReflectData.get().getSchema(classOf[TestData])
    print(schema)

    env.addSource(new SlowEmitSource()).print()

    env.execute("Demo Consumer: Load Schema From External Schema Register and Send to Kafka")
  }
}