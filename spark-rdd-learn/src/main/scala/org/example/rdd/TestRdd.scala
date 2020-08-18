package org.example.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by atyongsi@163.com on 2020/8/17
 * Description:Have Not Yet
 */
object TestRdd {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sparkContext = new SparkContext(conf)

    // TODO 在集群模式下运行结果是0,函数分发到不同的executor执行,对driver端的变量没有影响
    //如果希望Executor的计算能修改driver端的变量,需要引入累加器
    val data: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4))
    var init = 0
    data.foreach(x => init += x)

    println(s"init value: $init")

    sparkContext.stop
  }
}
