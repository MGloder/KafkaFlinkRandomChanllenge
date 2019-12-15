package com.machinedoll.experiment.processor

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.lang
import java.util.Properties

import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

object TestDataKafkaAvroSink {
  def getKafkaAvroSink[TestData](topic: String): FlinkKafkaProducer[TestData] = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "40.85.152.231:9092")
    props.setProperty("zookeeper.connect", "40.85.152.231:2181")
    props.setProperty("group.id", "example-group")


    new FlinkKafkaProducer[TestData](topic, new KafkaSerializationSchema[TestData] {
      override def serialize(t: TestData, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(stream)
        oos.writeObject(t)
        oos.close()
        val result = stream.toByteArray
        new ProducerRecord[Array[Byte], Array[Byte]](topic, result)
      }
    }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
  }
}
