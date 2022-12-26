package cn.jxau.strategy


import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object MyStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    println("Hello world!")
    Nil
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\devlop\\envs\\hadoop-common-2.2.0-bin-master");
    val spark = SparkSession.builder().master("local").getOrCreate()

    spark.experimental.extraStrategies = Seq(MyStrategy)
    val q = spark.catalog.listTables.filter(t => t.name == "six")
    q.explain(true)
    spark.stop()
  }
}