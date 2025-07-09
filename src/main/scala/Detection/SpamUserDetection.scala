package Detection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, when, window}
import org.apache.spark.sql.types._

object SpamUserDetection {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
      .config("spark.sql.shuffle.partitions", "200")
      .appName("Spam Detection")
      .getOrCreate()

    //Schemas - Comment, User, Post
    val userSchema = StructType(
      StructField("user_id", StringType, false) ::
      StructField("username", StringType, false) ::
      StructField("name", StringType, false) ::
      StructField("flaggedSpammer", IntegerType, true)::
        Nil
    )

    val commentSchema = new StructType()
      .add("user_id", IntegerType, false)
      .add("comment_id", IntegerType, false)
      .add("post_id", IntegerType, false)
      .add("text", StringType, false)
      .add("timestamp", LongType, false)

    val userList = spark.read.option("header", false).schema(userSchema).csv("/Users/mw-sanskar/Documents/SpamDetection/users_500.csv").toDF()

    val commentStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "comments")
      .option("failOnDataLoss", "false")
      .load()

    val comments = commentStream
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), commentSchema).as("data"))
      .select("data.*")
      .withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))

    val suspiciousUsers = comments
      .withWatermark("timestamp", "1 minute")
      .groupBy(
        window(col("timestamp"), "1 minute"),
        col("user_id")
      )
      .count()
      .filter(col("count") > 5)
      .select(
        col("user_id")
      )

    //now if user is already flagged then mark as spammer if not then mark him flagged i.e. warn him
    val spamUser = userList.join(
      suspiciousUsers,
      Seq("user_id"),
      "inner"
      )
      .withColumn("spammer", when(col("flaggedSpammer") > 0, 2).otherwise(1))
      .select(col("user_id"), col("spammer"))

    val streamUsers = spamUser
      .selectExpr("to_json(struct(*)) as value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "spammers")
      .option("checkpointLocation", "/Users/mw/Documents/KafkaLearning/SpamDetection/v2")
      .start()

    streamUsers.awaitTermination()




  }

}
