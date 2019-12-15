package com.machinedoll.experiment.data

import java.time.Instant

case class TestData(
                     string: String,
                     int: Int,
                     bigDecimal: BigDecimal,
                     instant: Instant,
                     nested: TestDataNested,
                     option: Option[String],
                     list: List[String],
                     map: Map[String, TestDataNested]
                   )

