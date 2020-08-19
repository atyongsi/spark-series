package org.example.spark

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

/**
 * Created by atyongsi@163.com on 2020/8/18
 * Description:Have Not Yet
 */
object TestSparkStreamingKafka {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("").setMaster("")
    val ssc = new StreamingContext(conf, Seconds(2))

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092,host2:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "use_a_separate_group_id_for_each_stream",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )
    val topics: Array[String] = Array("topicA", "topicB")

    val stream: InputDStream[ConsumerRecord[Nothing, Nothing]] = KafkaUtils.createDirectStream(
      ssc,
      //这里有三个可选项,PreferBrokers,PreferConsistent,PreferFixed
      //PreferBrokers:任务优先分配给kafka Broker节点上的executor处理
      //PreferConsistent:一致性的方式分配给所有节点的executor,大多数情况选择这种方式
      //PreferFixed:如果负载不均衡,通过这种方式手动指定分配,不在map里面的节点,默认PreferConsistent方式
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>

      //这里是业务代码,rdd的操作
      //...........

      //获取当前offset [注意:offset由多个维度来描述,topic+partition+fromOffset...]
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }

      //保存offset,可以通过CheckPoint/kafka本身/zookeeper/HBase/Redis/Mysql....等保存offset
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges) //保存在kafka里,异步提交offset
    }

  }

}
