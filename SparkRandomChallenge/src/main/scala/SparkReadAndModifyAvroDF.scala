import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkReadAndModifyAvroDF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkAvro")
      .getOrCreate()

    import spark.implicits._

    val users: DataFrame = spark
      .read
      .format("avro")
      .load("./SparkRandomChallenge/src/main/resources/users.avro")

    val userName = users.map(_.getString(0))

    userName.show()
  }
}
