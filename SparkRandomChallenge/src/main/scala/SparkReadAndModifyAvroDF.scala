import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkReadAndModifyAvroDF {

  case class Users(name: String, favorite_color: String, favorite_numbers: Array[Int])

  val userExampleSchema = StructType(
    Array(
      StructField("name", StringType, true),
      StructField("favorite_color", StringType, true),
      StructField("favorite_numbers", ArrayType(IntegerType, true), true)
    )
  )

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
      .schema(userExampleSchema)
      .load("./SparkRandomChallenge/src/main/resources/users.avro")

    val userName = users.map(_.getString(0))

    val userDS = users.as[Users]

    users
      .write
      .mode("overwrite")
      .format("avro")
      .save("./SparkRandomChallenge/src/main/resources/user_name.avro")

    userDS.show()
    //    userName.show()
  }
}

