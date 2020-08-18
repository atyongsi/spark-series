package org.example.dstreamTodf

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
 * Created by atyongsi@163.com on 2020/8/18
 * Description:DStream遍历RDD时,通过DF/DS计算RDD
 */
object SqlNetworkWordCount {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    // Create the context with a 2 second batch size
    val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))

    words.foreachRDD {
      (rdd: RDD[String], time: Time) =>

        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        import spark.implicits._

        val wordsDataFrame = rdd.map(w => Record(w)).toDF()

        wordsDataFrame.createOrReplaceTempView("words")

        val wordCountsDataFrame =
          spark.sql("select word, count(*) as total from words group by word")
        println(s"========= $time =========")
        wordCountsDataFrame.show()

    }
    ssc.start()
    ssc.awaitTermination()

  }
}

// 通过case class,让RDD转化成 DataFrame
case class Record(word: String)

// 单例模式的SparkSession,
object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }

}