package kafkaConnecter

import cakesolutions.kafka.KafkaProducer
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json.Writes

object ProducerFactory {

  import Serializer.JsonSerializer

  def apply[T](bootStrapServer: String)(implicit Writes: Writes[T]): KafkaProducer[String, T] = {

    lazy val kafkaProducerConf = KafkaProducer.Conf(
      bootstrapServers = bootStrapServer,
      keySerializer = new StringSerializer,
      valueSerializer = new JsonSerializer[T]
    )
    KafkaProducer(kafkaProducerConf)
  }
}
