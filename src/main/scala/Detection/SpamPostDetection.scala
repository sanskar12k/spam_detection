package Detection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, col, from_json, window}
import org.apache.spark.sql.types._

object SpamPostDetection {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
      .config("spark.sql.shuffle.partitions", "200")
      .appName("Spam Detection")
      .getOrCreate()

    //Schemas - Comment, User
    val userSchema = new StructType()
      .add("user_id", StringType, false)
      .add("spammer", IntegerType, false)
      .add("count", IntegerType, false)
      .add("window_start", TimestampType, false)

    val commentSchema = new StructType()
      .add("user_id", IntegerType, false)
      .add("comment_id", IntegerType, false)
      .add("post_id", IntegerType, false)
      .add("text", StringType, false)
      .add("timestamp", LongType, false)

    val SpammersStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "spammers")
      .option("failOnDataLoss", "false")
      .load()

    val spammers = SpammersStream
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), userSchema).as("data"))
      .select("data.*")

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

    val flaggedComments = comments
      .withWatermark("timestamp", "1 minute")
      .join(spammers, Seq("user_id"), "inner")

    val flaggedPosts = flaggedComments
      .withWatermark("timestamp", "1 minute")
      .groupBy(
        window(col("timestamp"), "1 minute"),
        col("post_id")
      )
      .agg(
        approx_count_distinct("user_id").alias("unique_user_count")
      )
      .filter(col("unique_user_count") > 3)
      .select(col("post_id"), col("unique_user_count"))


    val streamPost = flaggedPosts
      .selectExpr("to_json(struct(*)) as value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "flaggedPosts")
      .option("checkpointLocation", "/Users/mw/Documents/KafkaLearning/SpamDetection/v5")
      .start()

    streamPost.awaitTermination()

  }

}
