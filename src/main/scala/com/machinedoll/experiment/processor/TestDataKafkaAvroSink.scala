package com.machinedoll.experiment.processor

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.lang
import java.util.Properties

import com.machinedoll.experiment.data.TestData
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object TestDataKafkaAvroSink {
  def getSimpleString(topic: String): FlinkKafkaProducer[String] = {
    new FlinkKafkaProducer[String](
      "192.168.0.108:9092", // broker list
      topic, // target topic
      new SimpleStringSchema()); // serialization schema
  }

//  def getKafkaAvroSink(topic: String): FlinkKafkaProducer[TestData] = {
//
//    val props = new Properties()
//    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.108:9092")
//    //    props.put(ProducerConfig.ACKS_CONFIG, "all")
//    //    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
//    //    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
//    //    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.0.108:8081")
//    props.setProperty("group.id", "example-group")
//
//    //    public FlinkKafkaProducer (brokerList: String, topicId: String, serializationSchema: KeyedSerializationSchema[IN]) {
//    //    new FlinkKafkaProducer("")
//    //    public FlinkKafkaProducer (brokerList: String, topicId: String, serializationSchema: SerializationSchema[IN]) {
//
//    new FlinkKafkaProducer[TestData]("192.168.0.108:9092", "topic", new SerializationSchema[TestData] {
//      override def serialize(t: TestData): Array[Byte] = {
//        val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
//        val oos = new ObjectOutputStream(stream)
//        oos.writeObject(t)
//        oos.close()
//        stream.toByteArray
//      }
//    })
//
//
//  }
}
