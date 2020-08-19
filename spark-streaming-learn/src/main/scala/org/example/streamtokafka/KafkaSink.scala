package org.example.streamtokafka

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}


/**
 * Created by atyongsi@163.com on 2020/8/19
 * Description:SparkStreaming消费kafka数据,
 * 经过处理后写回到 Kafka的另一个topic
 */
// 封装 KafkaProducer,方便广播
class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {

  /* This is the key idea that allows us to work around running into
     NotSerializableExceptions. */
  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] = {
    producer.send(new ProducerRecord[K, V](topic, key, value))
  }

  def send(topic: String, value: V): Future[RecordMetadata] = {
    producer.send(new ProducerRecord[K, V](topic, value))
  }
}

object KafkaSink {

  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        // Ensure that, on executor JVM shutdown, the Kafka producer sends
        // any buffered messages to Kafka before shutting down.
        producer.close()
      }
      producer
    }
    new KafkaSink(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
}
