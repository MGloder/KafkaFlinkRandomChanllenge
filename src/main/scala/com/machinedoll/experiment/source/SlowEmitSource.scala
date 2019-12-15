package com.machinedoll.experiment.source

import java.time.Instant

import com.machinedoll.experiment.data.{TestData, TestDataNested}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.util.Random

/*
  Need load external schema
 */
class SlowEmitSource(sleepIntervalMS: Long) extends RichSourceFunction[TestData] {
  var isRunning = true

  def generateData(): TestData = TestData(
    string = "id",
    int = Random.nextInt(1000),
    bigDecimal = BigDecimal(Random.nextDouble()),
    instant = Instant.now(),
    nested = TestDataNested(1234),
    option = Some("option"),
    list = List("list"),
    map = Map("a" -> TestDataNested(0), "b" -> TestDataNested(1))
  )

  override def run(ctx: SourceFunction.SourceContext[TestData]): Unit = {
    while (isRunning) {
      val generatedData: TestData = generateData()
      ctx.markAsTemporarilyIdle()
      Thread.sleep(sleepIntervalMS)
      ctx.collect(generatedData)
    }
  }

  override def cancel(): Unit = isRunning = false
}
