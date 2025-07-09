package Mockdata

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random

object SpamDataSimulate {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("acks", "all")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)


  def generateTransaction(): String = {
    val userIds = List(1880, 1324, 1279, 1388, 1270, 1849, 1200, 1093, 1429, 1200, 1967, 1275, 1583, 1314, 1638, 1370)

    val openings = Array(
      "Check this out", "Visit now", "Click the link", "Limited offer", "Get rich fast",
      "Win big", "Earn $$$", "Don't miss", "Instant access", "Free entry"
    )

    val bodies = Array(
      "for exclusive deals", "before it's too late", "to see amazing content",
      "to make easy money", "and change your life", "to claim your prize",
      "guaranteed results", "no registration needed", "in under 24 hours", "with no effort"
    )


    val sentence = s"${openings(Random.nextInt(openings.length))} " +
      s"${bodies(Random.nextInt(bodies.length))}"


    s"""{
       |"user_id": ${userIds(Random.nextInt(userIds.length))},
       |"comment_id": ${1000 + Random.nextInt(1001)},
       |"post_id": ${1000 + Random.nextInt(1001)},
       |"text": "${sentence}",
       |"timestamp": ${System.currentTimeMillis()}
       |}""".stripMargin
  }

  def main(args: Array[String]): Unit = {
    // Send multiple transactions in quick succession
    for(_ <- 1 to 2000) {
      val comments = generateTransaction()
      println(comments)
      producer.send(new ProducerRecord[String, String]("comments", null, comments))
      Thread.sleep(250) // Random sleep between 0-500ms
    }

    producer.close()
  }
}

