package kafkaConnecter

import java.util

import model.{AreaDensity, Coordinate, HeavyTrafficArea}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import play.api.libs.json.{Format, Json, Writes}

object Serializer {

  implicit val coordinateFormat: Format[Coordinate] = Json.format[Coordinate]
  implicit val heavyTrafficAreaFormat: Format[HeavyTrafficArea] = Json.format[HeavyTrafficArea]
  implicit val coordinateDensityPairFormat: Format[(Coordinate, Long)] = Json.format[(Coordinate, Long)]
  implicit val areaDensityFormat: Format[AreaDensity] = Json.format[AreaDensity]

  class JsonSerializer[A: Writes] extends Serializer[A] {

    private val stringSerializer = new StringSerializer

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
      stringSerializer.configure(configs, isKey)

    override def serialize(topic: String, data: A): Array[Byte] =
      stringSerializer.serialize(topic, Json.stringify(Json.toJson(data)))

    override def close(): Unit =
      stringSerializer.close()
  }
}