package org.example.spark

import com.google.common.eventbus.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils

/**
 * Created by atyongsi@163.com on 2020/8/18
 * Description:Have Not Yet
 */
object TestSparkStreamingKafka {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("").setMaster("")
    val ssc = new StreamingContext(conf, Seconds(2))

    Map[String, Object](
      "bootstrap.server" -> "localhost:9092,host2:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics: Array[String] = Array("topicA", "topicB")


  }

}
