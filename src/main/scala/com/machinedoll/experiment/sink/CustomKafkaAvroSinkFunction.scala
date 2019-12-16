package com.machinedoll.experiment.sink

import com.machinedoll.experiment.data.TestData
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaInternalProducer

//class CustomKafkaAvroSinkFunction extends TwoPhaseCommitSinkFunction[TestData, TXN, CONTEXT] {
//
//}

