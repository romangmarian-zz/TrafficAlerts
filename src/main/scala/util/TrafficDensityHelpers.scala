package util

import java.util.Date

import cakesolutions.kafka.KafkaProducerRecord
import com.peertopark.java.geocalc.{EarthCalc, Point}
import com.typesafe.config.Config
import kafkaConnecter.ProducerFactory
import model.{AreaDensity, CompletedStep, Coordinate, HeavyTrafficArea}
import model.Constants._
import org.apache.spark.streaming.kafka010.OffsetRange

object TrafficDensityHelpers {

  import kafkaConnecter.Serializer._

  def getOffsetRanges(topic: String, partitions: List[Int], fromOffsets: List[Long],
                      toOffsets: List[Long]): Array[OffsetRange] =

    (partitions, fromOffsets, toOffsets).zipped.toList.map { combination =>
      OffsetRange.create(topic, combination._1, combination._2, combination._3)
    }.toArray

  def buildAreaBoxes(northExtreme: Double, southExtreme: Double, eastExtreme: Double, westExtreme: Double,
                     step: Double): (List[Point], Point) = {


    val upperLeftCorner = Point.build(northExtreme, westExtreme)
    val lowerLeftCorner = Point.build(southExtreme, westExtreme)
    val upperRightCorner = Point.build(northExtreme, eastExtreme)

    val areaCenter = EarthCalc.pointRadialDistance(lowerLeftCorner,
      EarthCalc.getBearing(lowerLeftCorner, upperRightCorner),
      EarthCalc.getDistance(lowerLeftCorner, upperRightCorner) / 2)

    def verticalStopCondition(lower: Point, upper: Point): Boolean = lower.getLatitude >= upper.getLatitude

    def horizontalStopCondition(left: Point, right: Point): Boolean = left.getLongitude >= right.getLongitude

    val leftColumn = getSubAreasCenters(upperLeftCorner, lowerLeftCorner, step, verticalStopCondition)
    val rightColumn = leftColumn.map(point => Point.build(point.getLatitude, eastExtreme))

    val subAreasCenters = (leftColumn, rightColumn).zipped.toList.flatMap { ends =>
      getSubAreasCenters(ends._1, ends._2, step, horizontalStopCondition)
    }

    (subAreasCenters, areaCenter)
  }

  private def getSubAreasCenters(start: Point, end: Point, step: Double,
                                 stopCondition: (Point, Point) => Boolean): List[Point] = {

    val bearing = EarthCalc.getBearing(start, end)

    @scala.annotation.tailrec
    def getNextCenters(current: Point, previousPoints: List[Point]): List[Point] = {

      if (stopCondition(current, end)) previousPoints
      else {
        val newPoint = EarthCalc.pointRadialDistance(current, bearing, step)
        getNextCenters(newPoint, previousPoints :+ newPoint)
      }
    }

    getNextCenters(start, List(start))
  }

  def getSubareasHashMap(points: List[Point], center: Point, step: Double): Map[Int, List[Point]] =
    points.groupBy(nbOfStepsFromCenter(_, center, step))

  def nbOfStepsFromCenter(point: Point, center: Point, step: Double): Int = {

    (EarthCalc.getDistance(point, center) / step).floor.toInt
  }

  def toEmptyMap(points: List[Point]): Map[Point, Long] = points.map(_ -> 0L).toMap

  def addEntryToSubareasMap(point: Point, subareasMap: Map[Point, Long], subAreasHashMap: Map[Int, List[Point]],
                            center: Point, step: Double): Map[Point, Long] = {

    val distanceFromCenter = nbOfStepsFromCenter(point, center, step)
    if (subAreasHashMap.contains(distanceFromCenter)) {
      val possibleSubAreas = subAreasHashMap(nbOfStepsFromCenter(point, center, step))
      val subArea = possibleSubAreas.minBy(EarthCalc.getDistance(_, point))
      subareasMap + (subArea -> (subareasMap(subArea) + 1))
    }
    else subareasMap
  }

  def combineSubAreasMaps(subAreaMap1: Map[Point, Long], subAreaMap2: Map[Point, Long]): Map[Point, Long] = {

    val merged: Seq[(Point, Long)] = subAreaMap1.toSeq ++ subAreaMap2.toSeq
    val grouped: Map[Point, Seq[(Point, Long)]] = merged.groupBy(_._1)
    val cleaned: Map[Point, List[Long]] = grouped.mapValues(_.map(_._2).toList)
    cleaned.mapValues(_.sum)
  }

  def toPoint(step: CompletedStep): Point = Point.build(step.location.latitude, step.location.longitude)

  def toCoordinate(point: Point): Coordinate = Coordinate(point.getLatitude, point.getLongitude)

  def inTimeWindow(startTimeS: String, endTimeS: String, timeL: Long): Boolean = {

    val startTime = TIME_FORMAT.parse(startTimeS)
    val endTime = TIME_FORMAT.parse(endTimeS)
    val time = new Date(timeL)

    startTime.compareTo(time) <= 0 && endTime.compareTo(time) >= 0
  }

  def recordAreaDensity(north: Double, south: Double, east: Double, west: Double, startTime: String, endTime: String,
                        subAreasDensitiesMap: Map[Point, Long])(implicit config: Config): Unit = {

    val producer = ProducerFactory[AreaDensity](config.getString("trafficDensity.kafka.bootstrap.server"))
    val topic = config.getString("trafficDensity.kafka.output.topic")

    val subAreaDensities = subAreasDensitiesMap.toList.map { case (point, density) =>
      (Coordinate(point.getLatitude, point.getLongitude), density)
    }
    val areaDensity = AreaDensity(north, south, east, west, startTime, endTime, subAreaDensities)

    val kafkaKey = s"$north;$south;$east;$west;$startTime;$endTime"

    producer.send(
      KafkaProducerRecord[String, AreaDensity](topic, kafkaKey, areaDensity)
    )
    producer.close()
  }
}
