package org.example.getstart

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

/**
 * Created by atyongsi@163.com on 2020/8/18
 * Description:通过 RDD[Row]和 StructType方式创建 DF/DS
 */
object Season {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession =
      SparkSession.builder
        .appName("Season")
        .master("local[2]")
        .getOrCreate()

    import spark.implicits._

    val dataRDD: RDD[String] = spark.sparkContext.textFile("C:\\workDir\\ideaCode\\myGithub\\spark-series\\spark-sql-learn\\src\\main\\resources\\season.txt")
    //转成RDD[Row]
    val dataRowRDD: RDD[Row] = dataRDD.map(line => {
      val fields: Array[String] = line.split(" ")
      Row(fields(0).toInt, fields(1), fields(2).toDouble)
    })

    //构造StructType,相当于创建表并建字段名称和类型
    val schema: StructType = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("season", StringType, false),
        StructField("like", DoubleType, false)
      ))

    val dataFrame: DataFrame = spark.createDataFrame(dataRowRDD, schema)

    dataFrame.select("season").orderBy("like").show
  }

}
