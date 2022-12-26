package cn.jxau.parser

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.parser.ParserInterface

object MyParserApp {
  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir","E:\\devlop\\envs\\hadoop-common-2.2.0-bin-master");
    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
    type ExtensionsBuilder = SparkSessionExtensions => Unit
    val parserBuilder: ParserBuilder = (_, parser) => new MyParser(parser)
    val extBuilder: ExtensionsBuilder = { e => e.injectParser(parserBuilder)}
    val spark =  SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[*]")
      .withExtensions(extBuilder)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = Seq(
      ( "First Value",1, java.sql.Date.valueOf("2010-01-01")),
      ( "First Value",4, java.sql.Date.valueOf("2010-01-01")),
      ("Second Value",2,  java.sql.Date.valueOf("2010-02-01")),
      ("Second Value",9,  java.sql.Date.valueOf("2010-02-01"))
    ).toDF("name", "score", "date_column")
    df.createTempView("p")

    //    val df = spark.read.json("examples/src/main/resources/people.json")
    //    df.toDF().write.saveAsTable("person")
    //,javg(score)

    // custom parser
    //    spark.sql("select * from p ").show

    spark.sql("select * from p").show()
  }
}