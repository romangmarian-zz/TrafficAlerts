import model.CompletedStep
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import util.Deserializer.JsonDeserializerWrapper
import org.apache.spark._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Boot {

  def main(args: Array[String]): Unit = {

    val sparkConfig =
      new SparkConf().setMaster("local[*]").setAppName("SparkKafkaStreamTest")
    val sparkStreamingContext = new StreamingContext(sparkConfig, Seconds(10))
    sparkStreamingContext.sparkContext.setLogLevel("ERROR")

    val kafkaConfig = Map[String, Object](
      "client.dns.lookup" -> "resolve_canonical_bootstrap_servers_only",
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[JsonDeserializerWrapper],
      "group.id" -> "kafkaSparkTestGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaTopics = Array("completed-steps")

    val kafkaRawStream: InputDStream[ConsumerRecord[String, CompletedStep]] =
      KafkaUtils.createDirectStream[String, CompletedStep](
        sparkStreamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, CompletedStep](kafkaTopics, kafkaConfig)
      )

    val weatherStream: DStream[String] =
      kafkaRawStream map (streamRawRecord =>streamRawRecord.value.toString)

    val recordsCount: DStream[Long] = weatherStream.count()

    weatherStream.print()


    sparkStreamingContext.start() // start the computation
    sparkStreamingContext.awaitTermination() // await termination
  }
}

// scalastyle:on println

