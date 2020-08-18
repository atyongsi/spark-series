package org.example.checkpoint

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by atyongsi@163.com on 2020/8/18
 * Description:给SparkStreaming的元数据设置检查点
 */
object MetadataCheckPoint {

  def main(args: Array[String]): Unit = {

    StreamingContext.getOrCreate("", functionToCreateContext)

  }

  def functionToCreateContext(): StreamingContext = {

    val checkDir: String = ""
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))

    ssc.checkpoint(checkDir)
    ssc
  }


}
