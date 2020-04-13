import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkReadAvroFromFile {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkAvro")
      .getOrCreate()

    val users: DataFrame = spark
      .read
      .format("avro")
      .load("./SparkRandomChallenge/src/main/resources/users.avro")

    users.printSchema()
  }
}
