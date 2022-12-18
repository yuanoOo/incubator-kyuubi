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

package org.apache.kyuubi.jdbc

import java.sql.SQLTimeoutException
import java.util.Properties

import org.apache.kyuubi.IcebergSuiteMixin
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.engine.spark.shim.SparkCatalogShim
import org.apache.kyuubi.jdbc.hive.{KyuubiConnection, KyuubiStatement}
import org.apache.kyuubi.tags.IcebergTest

@IcebergTest
class KyuubiHiveDriverSuite extends WithSparkSQLEngine with IcebergSuiteMixin {

  override def withKyuubiConf: Map[String, String] = extraConfigs -
    "spark.sql.defaultCatalog" -
    "spark.sql.catalog.spark_catalog"

  override def afterAll(): Unit = {
    super.afterAll()
    for ((k, _) <- withKyuubiConf) {
      System.clearProperty(k)
    }
  }

  test("get tables with kyuubi driver") {
    val driver = new KyuubiHiveDriver()
    val connection = driver.connect(getJdbcUrl, new Properties())
    assert(connection.getClass.getName === "org.apache.kyuubi.jdbc.hive.KyuubiConnection")
    val metaData = connection.getMetaData
    assert(metaData.getClass.getName === "org.apache.kyuubi.jdbc.hive.KyuubiDatabaseMetaData")
    val statement = connection.createStatement()
    val table1 = s"${SparkCatalogShim.SESSION_CATALOG}.default.kyuubi_hive_jdbc"
    val table2 = s"$catalog.default.hdp_cat_tbl"
    try {
      statement.execute(s"CREATE TABLE $table1(key int) USING parquet")
      statement.execute(s"CREATE TABLE $table2(key int) USING $format")

      val resultSet1 = metaData.getTables(SparkCatalogShim.SESSION_CATALOG, "default", "%", null)
      assert(resultSet1.next())
      assert(resultSet1.getString(1) === SparkCatalogShim.SESSION_CATALOG)
      assert(resultSet1.getString(2) === "default")
      assert(resultSet1.getString(3) === "kyuubi_hive_jdbc")

      val resultSet2 = metaData.getTables(catalog, "default", "%", null)
      assert(resultSet2.next())
      assert(resultSet2.getString(1) === catalog)
      assert(resultSet2.getString(2) === "default")
      assert(resultSet2.getString(3) === "hdp_cat_tbl")
    } finally {
      statement.execute(s"DROP TABLE IF EXISTS $table1")
      statement.execute(s"DROP TABLE IF EXISTS $table2")
      statement.close()
      connection.close()
    }
  }

  test("deprecated KyuubiDriver also works") {
    val driver = new KyuubiDriver()
    val connection = driver.connect(getJdbcUrl, new Properties())
    assert(connection.getClass.getName === "org.apache.kyuubi.jdbc.hive.KyuubiConnection")
    val metaData = connection.getMetaData
    assert(metaData.getClass.getName === "org.apache.kyuubi.jdbc.hive.KyuubiDatabaseMetaData")
    val statement = connection.createStatement()
    try {
      val resultSet = statement.executeQuery(s"SELECT 1")
      assert(resultSet.next())
      assert(resultSet.getInt(1) === 1)
    } finally {
      statement.close()
      connection.close()
    }
  }

  test("add executeScala api for KyuubiStatement") {
    val driver = new KyuubiHiveDriver()
    val connection = driver.connect(getJdbcUrl, new Properties())
    val statement = connection.createStatement().asInstanceOf[KyuubiStatement]
    try {
      val code = """spark.sql("set kyuubi.operation.language").show(false)"""
      val resultSet = statement.executeScala(code)
      assert(resultSet.next())
      assert(resultSet.getString(1).contains("kyuubi.operation.language"))
    } finally {
      statement.close()
      connection.close()
    }
  }

  test("executeScala support timeout") {
    val driver = new KyuubiHiveDriver()
    val connection = driver.connect(getJdbcUrl, new Properties())
    val statement = connection.createStatement().asInstanceOf[KyuubiStatement]
    statement.setQueryTimeout(5)
    try {
      val code = """java.lang.Thread.sleep(500000L)"""
      val e = intercept[SQLTimeoutException] {
        statement.executeScala(code)
      }.getMessage
      assert(e.contains("Query timed out"))
    } finally {
      statement.close()
      connection.close()
    }
  }

  test("wrapable KyuubiConnection") {
    val driver = new KyuubiHiveDriver()
    val connection = driver.connect(getJdbcUrl, new Properties())
    assert(connection.isWrapperFor(classOf[KyuubiConnection]))
    val kyuubiConnection = connection.unwrap(classOf[KyuubiConnection])
    assert(kyuubiConnection != null)
    connection.close()
  }
}
