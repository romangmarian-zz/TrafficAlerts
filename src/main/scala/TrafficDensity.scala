import com.typesafe.config.{Config, ConfigFactory}
import kafkaConnecter.Deserializer.CompletedStepDeserializer
import model.CompletedStep
import util.TrafficDensityHelpers._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import util.TrafficDensityHelpers

import collection.JavaConverters._


object TrafficDensity {

  def main(args: Array[String]): Unit = {

    implicit val config: Config = ConfigFactory.load()

    val side = config.getDouble("trafficDensity.side")
    val bootstrapSever = config.getString("trafficAlerts.kafka.bootstrap.server")
    val inputTopic = config.getString("trafficAlerts.kafka.input.topic")

    val startTime = config.getString("trafficDensity.startTime")
    val endTime = config.getString("trafficDensity.endTime")

    val northExtreme = config.getDouble("trafficDensity.northExtreme")
    val southExtreme = config.getDouble("trafficDensity.southExtreme")
    val westExtreme = config.getDouble("trafficDensity.westExtreme")
    val eastExtreme = config.getDouble("trafficDensity.eastExtreme")

    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("Traffic Density")
    val sparkContext = new SparkContext(sparkConfig)

    val partitions = config.getIntList("trafficDensity.partitions").asScala.toList.map(_.toInt)
    val fromOffsets = config.getLongList("trafficDensity.fromOffsets").asScala.toList.map(_.toLong)
    val toOffsets = config.getLongList("trafficDensity.toOffsets").asScala.toList.map(_.toLong)

    val offsetRanges = TrafficDensityHelpers.getOffsetRanges(inputTopic, partitions, fromOffsets, toOffsets)

    val kafkaConfig = Map[String, Object](
      "client.dns.lookup" -> "resolve_canonical_bootstrap_servers_only",
      "bootstrap.servers" -> bootstrapSever,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[CompletedStepDeserializer],
      "group.id" -> "kafkaSparkTestGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stepsRDD = KafkaUtils.createRDD[String, CompletedStep](
      sparkContext,
      kafkaConfig.asJava,
      offsetRanges,
      LocationStrategies.PreferConsistent)
      .map { stepRecord => stepRecord.value }

    val (subAreas, areaCenter) = buildAreaBoxes(northExtreme, southExtreme, eastExtreme, westExtreme, side)
    val subAreasHashMap = getSubareasHashMap(subAreas, areaCenter, side)

    val subAreaDensities = stepsRDD
      .filter(step => inTimeWindow(startTime, endTime, step.time))
      .aggregate(toEmptyMap(subAreas))((map, step) =>
        addEntryToSubareasMap(toPoint(step), map, subAreasHashMap, areaCenter, side),
        (map1, map2) => combineSubAreasMaps(map1, map2))

    subAreaDensities.foreach(x => Logger.getLogger("density").warn(x.toString))
    recordAreaDensity(northExtreme, southExtreme, eastExtreme, westExtreme, startTime, endTime, subAreaDensities)
  }
}
