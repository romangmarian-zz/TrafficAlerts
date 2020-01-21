package kafkaConnecter

import java.util

import model.{CompletedStep, Coordinate}
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
import play.api.libs.json.{Format, Json}

object Deserializer {

  implicit val coordinateFormat: Format[Coordinate] = Json.format[Coordinate]
  implicit val completedStepFormat: Format[CompletedStep] = Json.format[CompletedStep]

  class CompletedStepDeserializer extends Deserializer[CompletedStep] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = Unit

    override def deserialize(topic: String, data: Array[Byte]): CompletedStep = {
      val stringDeserializer = new StringDeserializer
      val result = Json.parse(stringDeserializer.deserialize(topic, data)).as[CompletedStep]
      stringDeserializer.close()
      result
    }

    override def close(): Unit = Unit

  }

}
