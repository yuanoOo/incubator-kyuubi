package cn.jxau

import org.apache.spark.sql.{Dataset, SparkSession}

object SparkTest {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("Listener-Test")
      .master("local[2]")
      .config("spark.extraListeners", "cn.jxau.MySparkAppListener")
      .config("spark.ui.showConsoleProgress", true)
      .getOrCreate()

    val lines: Dataset[String] = sparkSession.read.textFile("./README.md")

    lines.show(1)
  }
}
