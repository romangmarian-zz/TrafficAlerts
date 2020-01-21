package util

import cakesolutions.kafka.KafkaProducerRecord
import com.peertopark.java.geocalc.{EarthCalc, Point}
import com.typesafe.config.Config
import kafkaConnecter.ProducerFactory
import model.{CompletedStep, Coordinate, HeavyTrafficArea}

object TrafficAlertsHelpers {

  import kafkaConnecter.Serializer._

  def pointInRadius(origin: Coordinate, radiusInMeters: Double, currentPoint: Coordinate): Boolean = {

    val p1 = Point.build(origin.latitude, origin.longitude)
    val p2 = Point.build(currentPoint.latitude, currentPoint.longitude)
    val distance = EarthCalc.getDistance(p1, p2)
    distance <= radiusInMeters
  }

  def groupPointsByRadius(radius: Double)
                         (areas: Map[CompletedStep, Long], unit: CompletedStep): Map[CompletedStep, Long] = {

    if (areas.keys.map(_.unitId).toList.contains(unit.unitId))
      areas
    else {
      val surroundingArea = areas.keys.find(area => pointInRadius(area.location, radius, unit.location))
      surroundingArea match {
        case None => areas + (unit -> 1)
        case Some(areaCenter) => areas + (areaCenter -> (areas(areaCenter) + 1))
      }
    }
  }

  def combineMapsByRadius(radius: Double)
                         (map1: Map[CompletedStep, Long], map2: Map[CompletedStep, Long]): Map[CompletedStep, Long] = {

    map2.keys.foldLeft(map1)((aggregateMap, step) => {
      if (aggregateMap.keys.map(_.unitId).toList.contains(step.unitId)) map1
      else {
        val surroundingArea = aggregateMap.keys.find(area => pointInRadius(area.location, radius, step.location))
        surroundingArea match {
          case None => aggregateMap + (step -> map2(step))
          case Some(areaCenter) => aggregateMap + (areaCenter -> (aggregateMap(areaCenter) + map2(step)))
        }
      }
    })
  }

  def recordHeavyTraffic(areas: Map[Coordinate, Long])(implicit config: Config): Unit = {

    val producer = ProducerFactory[HeavyTrafficArea](config.getString("trafficAlerts.kafka.bootstrap.server"))
    val radius = config.getDouble("trafficAlerts.radius")
    val threshold = config.getInt("trafficAlerts.threshold")
    val topic = config.getString("trafficAlerts.kafka.output.topic")
    areas.foreach { case (origin, nbOfUnits) =>
      if (nbOfUnits > threshold) {
        val heavyTrafficArea = HeavyTrafficArea(origin, radius, nbOfUnits, System.currentTimeMillis())
        producer.send(
          KafkaProducerRecord[String, HeavyTrafficArea](topic, heavyTrafficArea.center.toString, heavyTrafficArea)
        )
        producer.close()
      }
    }
  }
}
