package cn.jxau.optimizer


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Add, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object MyOptimizer extends Rule[LogicalPlan] {

  def apply(logicalPlan: LogicalPlan): LogicalPlan = {
    logicalPlan.transformAllExpressions {
      case Add(left, right, false) => {
        println("this this my add optimizer")
        if (isStaticAdd(left)) {
          right
        } else if (isStaticAdd(right)) {
          Add(left, Literal(3L))
        } else {
          Add(left, right)
        }
      }
    }
  }

  private def isStaticAdd(expression: Expression): Boolean = {
    expression.isInstanceOf[Literal] && expression.asInstanceOf[Literal].toString == "0"
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\devlop\\envs\\hadoop-common-2.2.0-bin-master");
    val testSparkSession: SparkSession = SparkSession.builder().appName("Extra optimization rules")
      .master("local[*]")
      .withExtensions(extensions => {
        extensions.injectOptimizerRule(session => MyOptimizer)
      })
      .getOrCreate()

    testSparkSession.sparkContext.setLogLevel("ERROR")

    import testSparkSession.implicits._
    testSparkSession.experimental.extraOptimizations = Seq()
    Seq(-1, -2, -3).toDF("nr").write.mode("overwrite").json("./test_nrs")
    //    val optimizedResult = testSparkSession.read.json("./test_nrs").selectExpr("nr + 0")
    testSparkSession.read.json("./test_nrs").createTempView("p")

    var sql = "select nr+0 from p";
    var t = testSparkSession.sql(sql)
    println(t.queryExecution.optimizedPlan)
    println(sql)
    t.show()

    sql = "select 0+nr from p";
    var  u = testSparkSession.sql(sql)
    println(u.queryExecution.optimizedPlan)
    println(sql)
    u.show()

    sql = "select nr+8 from p";
    var  v = testSparkSession.sql(sql)
    println(v.queryExecution.optimizedPlan)
    println(sql)
    v.show()
    //    println(optimizedResult.queryExecution.optimizedPlan.toString() )
    //    optimizedResult.collect().map(row => row.getAs[Long]("(nr + 0)"))
    Thread.sleep(1000000)
  }

}