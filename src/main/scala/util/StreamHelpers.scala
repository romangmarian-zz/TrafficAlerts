package util

import model.{CompletedStep, Coordinate}
import com.peertopark.java.geocalc.EarthCalc
import com.peertopark.java.geocalc.Point

object StreamHelpers {

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

    map2.keys.foldLeft(map1)((map, step) => {
      if (map.keys.map(_.unitId).toList.contains(step.unitId)) map1
      else {
        val surroundingArea = map.keys.find(area => pointInRadius(area.location, radius, step.location))
        surroundingArea match {
          case None => map + (step -> map2(step))
          case Some(areaCenter) => map + (areaCenter -> (map(areaCenter) + map2(step)))
        }
      }
    })
  }
}
