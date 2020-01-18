import com.typesafe.config.{Config, ConfigFactory}
import model.CompletedStep
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import util.Deserializer.JsonDeserializerWrapper
import util.StreamHelpers._
import org.apache.spark._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Boot {

  def main(args: Array[String]): Unit = {

    implicit val config: Config = ConfigFactory.load()

    val radius: Long = config.getLong("radius")
    val bootstrapSever = config.getString("bootstrap.server")
    val inputTopic = config.getString("input.topic")

    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("Traffic Alerts")
    val sparkStreamingContext = new StreamingContext(sparkConfig, Seconds(10))

    sparkStreamingContext.sparkContext.setLogLevel("ERROR")

    val kafkaConfig = Map[String, Object](
      "client.dns.lookup" -> "resolve_canonical_bootstrap_servers_only",
      "bootstrap.servers" -> bootstrapSever,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[JsonDeserializerWrapper],
      "group.id" -> "kafkaSparkTestGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaTopics = Array(inputTopic)

    val kafkaRawStream: InputDStream[ConsumerRecord[String, CompletedStep]] =
      KafkaUtils.createDirectStream[String, CompletedStep](
        sparkStreamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, CompletedStep](kafkaTopics, kafkaConfig)
      )

    val stepsStream: DStream[CompletedStep] =
      kafkaRawStream map (streamRawRecord => streamRawRecord.value)

    stepsStream.foreachRDD(stepsRDD => {

      val aggregatedSteps =
        stepsRDD.aggregate(Map[CompletedStep, Long]())((map, step) => groupPointsByRadius(radius)(map, step),
        (map1, map2) => combineMapsByRadius(radius)(map1, map2))
      Logger.getLogger("log").error("current state" + aggregatedSteps.toString)
    })


    sparkStreamingContext.start() // start the computation
    sparkStreamingContext.awaitTermination() // await termination
  }

}

// scalastyle:on println

