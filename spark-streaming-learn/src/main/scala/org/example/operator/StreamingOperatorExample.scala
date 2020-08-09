package org.example.operator

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by atyongsi@163.com on 2020/8/7
 */
object StreamingOperatorExample {
  def main(args: Array[String]): Unit = {
    //前面一系列常规操作
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)
    val ssc = new StreamingContext(conf, Seconds(3))

    /* @param: [args]
     * @return: void
     * @description: SparkStreaming的数据源除了sockets，还可以读取files(文件).
     * 平时工作中主要以kafka的数据源为主,还可能是flume,kinesis.这里以读取file,伪代码
     */
    val dataDir = "xxx"
    val datas: DStream[String] = ssc.textFileStream(dataDir)
    val words: DStream[(String, Int)] = datas.flatMap(_.split(" ")).map(word => (word, 1))

    //这里读取另一个黑名单,在transform操作中过滤
    val blackListRdd: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(Array(("spamString", true)), 5)
    //transform,转换操作,这里用来过滤上步骤的黑名单
    val cleanDStream: DStream[Int] = words.transform(rdd => {
      val fliteredRDD: RDD[(String, (Int, Option[Boolean]))] = rdd.leftOuterJoin(blackListRdd).filter(tuple => {
        if (tuple._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })
      val validRDD: RDD[Int] = fliteredRDD.map(tuple => tuple._2._1)
      validRDD
    })

    //这个函数为下面updateStateStateByKey准备
    def updateStateFunction(currentValue: Seq[Int], historyValue: Option[Int]): Option[Int] = {
      val newValue = currentValue.sum + historyValue.getOrElse(0)
      Some(newValue)
    }

    //updateStateStateByKey,转换操作
    val runningCounts: DStream[(String, Int)] = words.updateStateByKey(updateStateFunction _)

    //reduceByKeyAndWindow,转换中的窗口操作.每10秒钟合并过去30秒的数据
    val result: DStream[(String, Int)] = words.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(30), Seconds(10))

    /*
     * @description: Stresm-stream join
     * Dstream操作
     * val stream1: DStream[String, String] = ...
     * val stream2: DStream[String, String] = ...
     * val joinedStream = stream1.join(stream2)
     * 窗口操作
     * val windowedStream1 = stream1.window(Seconds(20))
     * val windowedStream2 = stream2.window(Minutes(1))
     * val joinedStream = windowedStream1.join(windowedStream2)
     *
     */


    /* @param: [args]
     * @return: void
     * @description: Stream-dataset join
     *
     * val dataset: RDD[String, String] = ...
     * val windowedStream = stream.window(Seconds(20))...
     * val joinedStream = windowedStream.transform { rdd => rdd.join(dataset) }
     *
     */


    /* @param: [args]
     * @return: void
     * @description: Output算子,关于foreachRDD的设计模式
     *
     */
    //连接数据库的方法
    def createNewConnection() = {

    }

    /*第一种方法
     dstream.foreachRDD { rdd =>
       val connection = createNewConnection()   // 这一步在driver上执行
       rdd.foreach { record =>
         connection.send(record)  // 这一步在worker上执行
       }
     }
     上述代码是错误的,因为它需要把连接对象connection序列化，再从驱动器节点发送到worker节点.
     但是连接对象通常不能序列化,也不能跨节点传输
     */

    /*第二种方法
     dstream.foreachRDD { rdd =>
       rdd.foreach { record =>
         val connection = createNewConnection()
         connection.send(record)
         connection.close()
       }
     }
     把连接数据库的操作放在worker上执行,代码不会报错
     但是没发送一条消息,都要连接和关闭数据库,非常消耗资源
     */

    /*第三种方法
     dstream.foreachRDD { rdd =>
       rdd.foreachPartition { partitionOfRecords =>
         val connection = createNewConnection()
         partitionOfRecords.foreach(record => connection.send(record))
         connection.close()
       }
     }
    foreachPartition 将连接对象的创建开销摊到分区上进行(即每个分区连接一次数据库)，减少了资源的消耗。
    */

    /*第四种方法
    dstream.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>

        // ConnectionPool 是一个静态的、懒惰初始化的连接池
        val connection = ConnectionPool.getConnection()

        partitionOfRecords.foreach(record => connection.send(record))

        // 将连接返还给连接池，以便后续复用之
        ConnectionPool.returnConnection(connection)
      }
    }
    最好的方法是创建一个静态连接池，在需要连接的时候从连接池中取出连接，使用结束后放回连接池
    */

    //SparkStreaming的checkpoint
    //创建StreamingContext对象
    def createStreamingContext(): StreamingContext = {
      //      val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)
      val streamingContext = new StreamingContext(conf, Seconds(3))
      val value: ReceiverInputDStream[String] = streamingContext.socketTextStream(" ", 9999)
      //这里一系列操作
      streamingContext.checkpoint("checkpoint存储的位置")
      streamingContext
    }

    val context: StreamingContext = StreamingContext.getOrCreate("checkpoint存储的位置", createStreamingContext())
    //    context一系列操作
    context.start()
    context.awaitTermination()

  }

}
