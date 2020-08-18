package org.example.getstart

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Created by atyongsi@163.com on 2020/8/18
 * Description:通过 case class创建 DF/DS
 */
object Info {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession =
      SparkSession.builder
        .appName("Info")
        .master("local[2]")
        .getOrCreate()

    import spark.implicits._

    //通过case class创建Dataset
    val dataSet: Dataset[Info] = Seq(
      Info(9527, "wu"),
      Info(9529, "yong"),
      Info(9528, "si")
    ).toDS()

    dataSet.select("name").orderBy("id").show()
  }

  case class Info(id: Int, name: String)

}
