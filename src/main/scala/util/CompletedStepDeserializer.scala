package util

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

import model.{CompletedStep, Coordinate}
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
import play.api.libs.json.{Format, Json, Reads}

object CompletedStepProtocol {
  implicit val coordinateFormat: Format[Coordinate] = Json.format[Coordinate]
  implicit val completedStepFormat: Format[CompletedStep] = Json.format[CompletedStep]
}

class CompletedStepDeserializer extends Deserializer[CompletedStep] {

  override def deserialize(topic: String, bytes: Array[Byte]): CompletedStep = {
    val byteIn = new ByteArrayInputStream(bytes)
    val objIn = new ObjectInputStream(byteIn)
    val obj = objIn.readObject().asInstanceOf[CompletedStep]
    byteIn.close()
    objIn.close()
    obj
  }

  override def close(): Unit = {
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
  }

}

object Deserializer {

  implicit val coordinateFormat: Format[Coordinate] = Json.format[Coordinate]
  implicit val completedStepFormat: Format[CompletedStep] = Json.format[CompletedStep]

  class JsonDeserializer[A: Reads] extends Deserializer[A] {

    private val stringDeserializer = new StringDeserializer

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
      stringDeserializer.configure(configs, isKey)

    override def deserialize(topic: String, data: Array[Byte]): A =
      Json.parse(stringDeserializer.deserialize(topic, data)).as[A]

    override def close(): Unit =
      stringDeserializer.close()

  }

  class JsonDeserializerWrapper extends Deserializer[CompletedStep] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = Unit

    override def deserialize(topic: String, data: Array[Byte]): CompletedStep = {
      val jsonDeserializer = new JsonDeserializer[CompletedStep]
      val result = jsonDeserializer.deserialize(topic, data)
      jsonDeserializer.close()
      result
    }

    override def close(): Unit = Unit

  }

}
