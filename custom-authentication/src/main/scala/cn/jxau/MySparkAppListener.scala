package cn.jxau

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart}

class MySparkAppListener extends SparkListener with Logging {

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    val appId = applicationStart.appId
    applicationStart.driverLogs.foreach(x => println(x))
    logWarning("***************************************************" + appId.get)
    println("Start")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logError("************************ app end time ************************ " + applicationEnd.time)
    println("End")
  }
}