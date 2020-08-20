package org.example.kafkasource

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by atyongsi@163.com on 2020/8/20
 * Description:Have Not Yet
 */
object TestKafkaSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("")
      .appName("")
      .getOrCreate()

    val df: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.server", "host1:9092,host2:9092")
      .option("subscribe", "topic1,topic2") //("subscribePattern","topic.*")
      .option("includeHeaders", true)
      .load


  }

}
