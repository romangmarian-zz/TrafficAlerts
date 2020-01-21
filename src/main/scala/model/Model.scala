package model

case class Coordinate(
                       latitude: Double,
                       longitude: Double
                     )

case class CompletedStep(
                          unitId: String,
                          location: Coordinate,
                          time: Long
                        )

case class HeavyTrafficArea(
                             center: Coordinate,
                             radius: Double,
                             nbOfUnits: Long,
                             time: Long
                           )

case class AreaDensity(
                        north: Double,
                        south: Double,
                        east: Double,
                        west: Double,
                        startTime: String,
                        endTime: String,
                        subAreaDensities: List[(Coordinate, Long)]
                      )