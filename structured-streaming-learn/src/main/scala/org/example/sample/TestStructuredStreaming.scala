package org.example.sample

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Created by atyongsi@163.com on 2020/8/18
 * Description:Have Not Yet
 */
object TestStructuredStreaming {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("")
      .master("")
      .getOrCreate()

    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val value: Dataset[(String)] = lines.as[String].flatMap(_.split(" "))

    val wordCounts: DataFrame = value.groupBy("value").count()

    val query: StreamingQuery = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start

    query.awaitTermination()
  }

}
