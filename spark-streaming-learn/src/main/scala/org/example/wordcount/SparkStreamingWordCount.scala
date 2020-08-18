package org.example.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by atyongsi@163.com on 2020/8/7
 */
object SparkStreamingWordCount {
  def main(args: Array[String]): Unit = {
    //配置SparkConf信息，创建StreamingContext
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))

    //ssc监控socket的数据,获取数据
    val datas: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    //对读取的数据进行切分,统计数量
    val result: DStream[(String, Int)] = datas.flatMap(x => x.split(" "))
      .map(word => (word, 1)).reduceByKey(_ + _)

    result.print

    ssc.start() //开始计算
    ssc.awaitTermination() //等待计算终止
  }

}
