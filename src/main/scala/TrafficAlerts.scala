import com.typesafe.config.{Config, ConfigFactory}
import kafkaConnecter.Deserializer.CompletedStepDeserializer
import model.CompletedStep
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import util.TrafficAlertsHelpers._
import org.apache.spark._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object TrafficAlerts {

  def main(args: Array[String]): Unit = {

    implicit val config: Config = ConfigFactory.load()

    val radius: Long = config.getLong("trafficAlerts.radius")
    val bootstrapSever = config.getString("trafficAlerts.kafka.bootstrap.server")
    val inputTopic = config.getString("trafficAlerts.kafka.input.topic")
    val updateTime = config.getInt("trafficAlerts.kafka.batchUpdateTime")

    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("Traffic Alerts")
    val sparkStreamingContext = new StreamingContext(sparkConfig, Seconds(updateTime))

    sparkStreamingContext.sparkContext.setLogLevel("ERROR")

    val kafkaConfig = Map[String, Object](
      "client.dns.lookup" -> "resolve_canonical_bootstrap_servers_only",
      "bootstrap.servers" -> bootstrapSever,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[CompletedStepDeserializer],
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

      //for every unit, keep only the latest reached step
      val latestSteps = stepsRDD.groupBy(_.unitId).mapValues(_.maxBy(_.time)).values

      //aggregate steps by area
      val aggregatedSteps =
        latestSteps.aggregate(Map[CompletedStep, Long]())((map, step) => groupPointsByRadius(radius)(map, step),
          (map1, map2) => combineMapsByRadius(radius)(map1, map2))

      //transform completed steps to locations
      val areaTraffic = aggregatedSteps.map { case (key: CompletedStep, value: Long) => (key.location, value) }

      recordHeavyTraffic(areaTraffic)

      Logger.getLogger("log").debug("current steps" + latestSteps.count())
      Logger.getLogger("log").debug("aggSteps" + aggregatedSteps.keys.size)
      Logger.getLogger("log").debug("current state" + areaTraffic.toString)
    })

    sparkStreamingContext.start() // start the computation
    sparkStreamingContext.awaitTermination() // await termination
  }

}

// scalastyle:on println

