package org.example.getstart

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by atyongsi@163.com on 2020/8/18
 * Description:直接读取文件,构建 DF/DS
 */
object TestSparkSql {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession.builder
        .appName("TestSparkSql")
        .master("local[2]")
        .getOrCreate()

    import spark.implicits._

    //DSL语法
    val dataFrame: DataFrame =
      spark.read.json("C:\\workDir\\ideaCode\\myGithub\\spark-series\\spark-sql-learn\\src\\main\\resources\\person")
    dataFrame.select("age").show

    //SQL语法
    dataFrame.createGlobalTempView("people")
    val sqlDf: DataFrame =
      spark.sql("select * from global_temp.people")
    sqlDf.show
  }
}

