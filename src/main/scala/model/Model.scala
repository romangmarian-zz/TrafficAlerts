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