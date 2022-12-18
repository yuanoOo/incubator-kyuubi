/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.spark

import java.io.File
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.Duration
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import org.scalatest.time.SpanSugar._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.kyuubi.{KerberizedTestHelper, KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_LOG_TIMEOUT, ENGINE_SPARK_MAIN_RESOURCE}
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.service.ServiceUtils

class SparkProcessBuilderSuite extends KerberizedTestHelper with MockitoSugar {
  private def conf = KyuubiConf().set("kyuubi.on", "off")

  test("spark process builder") {
    val builder = new SparkProcessBuilder("kentyao", conf)
    val commands = builder.toString.split(' ')
    assert(commands(2) === "org.apache.kyuubi.engine.spark.SparkSQLEngine")
    assert(commands.contains("spark.kyuubi.on=off"))
    builder.toString.contains("\\\n\t--class")
    builder.toString.contains("\\\n\t--conf spark.kyuubi.on=off")
    val pb = new ProcessBuilder(commands.head, "--help")
    assert(pb.start().waitFor() === 0)
    assert(Files.exists(Paths.get(commands.last)))

    val process = builder.start
    assert(process.isAlive)
    process.destroyForcibly()
  }

  test("capture error from spark process builder") {
    val processBuilder = new SparkProcessBuilder("kentyao", conf.set("spark.ui.port", "abc"))
    processBuilder.start
    eventually(timeout(90.seconds), interval(500.milliseconds)) {
      val error = processBuilder.getError
      assert(error.getMessage.contains(
        "java.lang.IllegalArgumentException: spark.ui.port should be int, but was abc"))
      assert(error.isInstanceOf[KyuubiSQLException])
    }

    val processBuilder1 =
      new SparkProcessBuilder("kentyao", conf.set("spark.hive.metastore.uris", "thrift://dummy"))

    processBuilder1.start
    eventually(timeout(90.seconds), interval(500.milliseconds)) {
      val error1 = processBuilder1.getError
      assert(
        error1.getMessage.contains("org.apache.hadoop.hive.ql.metadata.HiveException:"))
    }
  }

  test("engine log truncation") {
    val pb =
      new SparkProcessBuilder("kentyao", conf.set("spark.hive.metastore.uris", "thrift://dummy"))
    pb.start
    eventually(timeout(90.seconds), interval(500.milliseconds)) {
      val error1 = pb.getError
      assert(!error1.getMessage.contains("Failed to detect the root cause"))
      assert(error1.getMessage.contains("See more: "))
    }

    val pb2 = new SparkProcessBuilder(
      "kentyao",
      conf.set("spark.hive.metastore.uris", "thrift://dummy")
        .set(KyuubiConf.ENGINE_ERROR_MAX_SIZE, 200))
    pb2.start
    eventually(timeout(90.seconds), interval(500.milliseconds)) {
      val error1 = pb2.getError
      assert(!error1.getMessage.contains("Failed to detect the root cause"))
      assert(error1.getMessage.contains("See more: "))
    }

    val pb3 =
      new SparkProcessBuilder("kentyao", conf.set("spark.kerberos.principal", testPrincipal))
    pb3.start
    eventually(timeout(90.seconds), interval(500.milliseconds)) {
      val error1 = pb3.getError
      assert(!error1.getMessage.contains("Failed to detect the root cause"))
      assert(error1.getMessage.contains("See more: "))
      assert(error1.getMessage.contains("Only one of --proxy-user or --principal can be provided"))
    }
  }

  test("proxy user or keytab") {
    val b1 = new SparkProcessBuilder("kentyao", conf)
    assert(b1.toString.contains("--proxy-user kentyao"))

    val conf1 = conf.set("spark.kerberos.principal", testPrincipal)
    val b2 = new SparkProcessBuilder("kentyao", conf1)
    assert(b2.toString.contains("--proxy-user kentyao"))

    val conf2 = conf.set("spark.kerberos.keytab", testKeytab)
    val b3 = new SparkProcessBuilder("kentyao", conf2)
    assert(b3.toString.contains("--proxy-user kentyao"))

    tryWithSecurityEnabled {
      val conf3 = conf.set("spark.kerberos.principal", testPrincipal)
        .set("spark.kerberos.keytab", "testKeytab")
      val b4 = new SparkProcessBuilder(Utils.currentUser, conf3)
      assert(b4.toString.contains(s"--proxy-user ${Utils.currentUser}"))

      val conf4 = conf.set("spark.kerberos.principal", testPrincipal)
        .set("spark.kerberos.keytab", testKeytab)
      val b5 = new SparkProcessBuilder("kentyao", conf4)
      assert(b5.toString.contains("--proxy-user kentyao"))

      val b6 = new SparkProcessBuilder(ServiceUtils.getShortName(testPrincipal), conf4)
      assert(!b6.toString.contains("--proxy-user kentyao"))
    }
  }

  test("log capture should release after close") {
    val process = new FakeSparkProcessBuilder(KyuubiConf())
    try {
      assert(process.logCaptureThreadReleased)
      val subProcess = process.start
      assert(!process.logCaptureThreadReleased)
      subProcess.waitFor(3, TimeUnit.SECONDS)
    } finally {
      process.close()
    }
    eventually(timeout(3.seconds), interval(100.milliseconds)) {
      assert(process.logCaptureThreadReleased)
    }
  }

  test(s"sub process log should be overwritten") {
    def atomicTest(): Unit = {
      val pool = Executors.newFixedThreadPool(3)
      val fakeWorkDir = Utils.createTempDir("fake")
      val dir = fakeWorkDir.toFile
      try {
        assert(dir.list().length == 0)

        val longTimeFile = new File(dir, "kyuubi-spark-sql-engine.log.1")
        longTimeFile.createNewFile()
        longTimeFile.setLastModified(System.currentTimeMillis() - 3600000)

        val shortTimeFile = new File(dir, "kyuubi-spark-sql-engine.log.0")
        shortTimeFile.createNewFile()

        val config = KyuubiConf().set(KyuubiConf.ENGINE_LOG_TIMEOUT, 20000L)
        (1 to 10).foreach { _ =>
          pool.execute(new Runnable {
            override def run(): Unit = {
              val pb = new FakeSparkProcessBuilder(config) {
                override val workingDir: Path = fakeWorkDir
              }
              try {
                val p = pb.start
                p.waitFor()
              } finally {
                pb.close()
              }
            }
          })
        }
        pool.shutdown()
        while (!pool.isTerminated) {
          Thread.sleep(100)
        }
        assert(dir.listFiles().length == 10 + 1)
      } finally {
        dir.listFiles().foreach(_.delete())
        dir.delete()
      }
    }

    // this loop is to promise we have a good test
    (1 to 10).foreach { _ =>
      atomicTest()
    }
  }

  test("overwrite log file should cleanup before write") {
    val fakeWorkDir = Utils.createTempDir("fake")
    val conf = KyuubiConf()
    conf.set(ENGINE_LOG_TIMEOUT, Duration.ofDays(1).toMillis)
    val builder1 = new FakeSparkProcessBuilder(conf) {
      override val workingDir: Path = fakeWorkDir
    }
    val file1 = builder1.engineLog
    Files.write(file1.toPath, "a".getBytes(), StandardOpenOption.APPEND)
    assert(file1.length() == 1)
    Files.write(file1.toPath, "a".getBytes(), StandardOpenOption.APPEND)
    assert(file1.length() == 2)
    file1.setLastModified(System.currentTimeMillis() - Duration.ofDays(1).toMillis - 1000)

    val builder2 = new FakeSparkProcessBuilder(conf) {
      override val workingDir: Path = fakeWorkDir
    }
    val file2 = builder2.engineLog
    assert(file1.getAbsolutePath == file2.getAbsolutePath)
    Files.write(file2.toPath, "a".getBytes(), StandardOpenOption.APPEND)
    assert(file2.length() == 1)
  }

  test("main resource jar should not check when is not a local file") {
    val workDir = Utils.createTempDir("resource")
    val jarPath = Paths.get(workDir.toString, "test.jar")
    val hdfsPath = s"hdfs://$jarPath"

    val conf = KyuubiConf()
    conf.set(ENGINE_SPARK_MAIN_RESOURCE, hdfsPath)
    val b1 = new SparkProcessBuilder("test", conf)
    assert(b1.mainResource.get.startsWith("hdfs://"))
    assert(b1.mainResource.get == hdfsPath)

    // user specified jar not exist, get default jar and expect not equals
    conf.set(ENGINE_SPARK_MAIN_RESOURCE, jarPath.toString)
    val b2 = new SparkProcessBuilder("test", conf)
    assert(b2.mainResource.getOrElse("") != jarPath.toString)
  }

  test("add spark prefix for conf") {
    val conf = KyuubiConf(false)
    conf.set("kyuubi.kent", "yao")
    conf.set("spark.vino", "yang")
    conf.set("kent", "yao")
    conf.set("hadoop.kent", "yao")
    val builder = new SparkProcessBuilder("", conf)
    val commands = builder.toString.split(' ')
    assert(commands.contains("spark.kyuubi.kent=yao"))
    assert(commands.contains("spark.vino=yang"))
    assert(commands.contains("spark.kent=yao"))
    assert(commands.contains("spark.hadoop.hadoop.kent=yao"))
  }

  test("zookeeper kerberos authentication") {
    val conf = KyuubiConf()
    conf.set(HighAvailabilityConf.HA_ZK_ENGINE_AUTH_TYPE.key, AuthTypes.KERBEROS.toString)
    conf.set(HighAvailabilityConf.HA_ZK_AUTH_KEYTAB.key, testKeytab)
    conf.set(HighAvailabilityConf.HA_ZK_AUTH_PRINCIPAL.key, testPrincipal)

    val b1 = new SparkProcessBuilder("test", conf)
    assert(b1.toString.contains(s"--conf spark.files=$testKeytab"))
  }

  test("SparkProcessBuilder commands immutable") {
    val conf = KyuubiConf(false)
    val engineRefId = UUID.randomUUID().toString
    val pb = new SparkProcessBuilder("", conf, engineRefId)
    assert(pb.toString.contains(engineRefId))
    val engineRefId2 = UUID.randomUUID().toString
    conf.set("spark.yarn.tags", engineRefId2)
    assert(!pb.toString.contains(engineRefId2))
    assert(pb.toString.contains(engineRefId))
  }

  test("SparkProcessBuilder build spark engine with SPARK_USER_NAME") {
    val proxyName = "kyuubi"
    val conf1 = KyuubiConf(false).set("spark.master", "k8s://test:12345")
    val b1 = new SparkProcessBuilder(proxyName, conf1)
    val c1 = b1.toString.split(' ')
    assert(c1.contains(s"spark.kubernetes.driverEnv.SPARK_USER_NAME=$proxyName"))
    assert(c1.contains(s"spark.executorEnv.SPARK_USER_NAME=$proxyName"))

    tryWithSecurityEnabled {
      val conf2 = conf.set("spark.master", "k8s://test:12345")
        .set("spark.kerberos.principal", testPrincipal)
        .set("spark.kerberos.keytab", testKeytab)
      val name = ServiceUtils.getShortName(testPrincipal)
      val b2 = new SparkProcessBuilder(name, conf2)
      val c2 = b2.toString.split(' ')
      assert(c2.contains(s"spark.kubernetes.driverEnv.SPARK_USER_NAME=$name"))
      assert(c2.contains(s"spark.executorEnv.SPARK_USER_NAME=$name"))
      assert(!c2.contains(s"--proxy-user $name"))
    }

    // Test no-kubernetes case
    val conf3 = KyuubiConf(false)
    val b3 = new SparkProcessBuilder(proxyName, conf3)
    val c3 = b3.toString.split(' ')
    assert(!c3.contains(s"spark.kubernetes.driverEnv.SPARK_USER_NAME=$proxyName"))
    assert(!c3.contains(s"spark.executorEnv.SPARK_USER_NAME=$proxyName"))
  }
}

class FakeSparkProcessBuilder(config: KyuubiConf)
  extends SparkProcessBuilder("fake", config) {
  override protected val commands: Array[String] = Array("ls")
}
