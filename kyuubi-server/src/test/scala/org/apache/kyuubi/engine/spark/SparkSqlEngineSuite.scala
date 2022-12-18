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

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class SparkSqlEngineSuite extends WithKyuubiServer with HiveJDBCTestHelper {
  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(SESSION_CONF_IGNORE_LIST.key, "kyuubi.abc.xyz,spark.sql.abc.xyz,spark.sql.abc.var")
      .set(SESSION_CONF_RESTRICT_LIST.key, "kyuubi.xyz.abc,spark.sql.xyz.abc,spark.sql.xyz.abc.var")
  }

  test("ignore config via system settings") {
    val sessionConf = Map("kyuubi.abc.xyz" -> "123", "kyuubi.abc.xyz0" -> "123")
    val sparkHiveConfs = Map("spark.sql.abc.xyz" -> "123", "spark.sql.abc.xyz0" -> "123")
    val sparkHiveVars = Map("spark.sql.abc.var" -> "123", "spark.sql.abc.var0" -> "123")
    withSessionConf(sessionConf)(sparkHiveConfs)(sparkHiveVars) {
      withJdbcStatement() { statement =>
        Seq("spark.sql.abc.xyz", "spark.sql.abc.var").foreach { key =>
          val rs1 = statement.executeQuery(s"SET ${key}0")
          assert(rs1.next())
          assert(rs1.getString("value") === "123")
          val rs2 = statement.executeQuery(s"SET $key")
          assert(rs2.next())
          assert(rs2.getString("value") === "<undefined>", "ignored")
        }
      }
    }
  }

  test("restricted config via system settings") {
    val sessionConfMap = Map("kyuubi.xyz.abc" -> "123", "kyuubi.abc.xyz" -> "123")
    withSessionConf(sessionConfMap)(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        sessionConfMap.keys.foreach { key =>
          val rs = statement.executeQuery(s"SET $key")
          assert(rs.next())
          assert(
            rs.getString("value") === "<undefined>",
            "session configs do not reach on server-side")
        }

      }
    }

    withSessionConf(Map.empty)(Map("spark.sql.xyz.abc" -> "123"))(Map.empty) {
      assertJDBCConnectionFail()
    }

    withSessionConf(Map.empty)(Map.empty)(Map("spark.sql.xyz.abc.var" -> "123")) {
      assertJDBCConnectionFail()
    }
  }

  test("Fail connections on invalid sub domains") {
    Seq("/", "/tmp", "", "abc/efg", ".", "..").foreach { invalid =>
      val sparkHiveConfigs = Map(
        ENGINE_SHARE_LEVEL.key -> "USER",
        ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> invalid)
      withSessionConf(Map.empty)(sparkHiveConfigs)(Map.empty) {
        assertJDBCConnectionFail()
      }
    }
  }

  test("Engine isolation with sub domain configurations") {
    val sparkHiveConfigs = Map(
      ENGINE_SHARE_LEVEL.key -> "USER",
      ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "spark",
      "spark.driver.memory" -> "1000M")
    var mem: String = null
    withSessionConf(Map.empty)(sparkHiveConfigs)(Map.empty) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery(s"SET spark.driver.memory")
        assert(rs.next())
        mem = rs.getString(2)
        assert(mem === "1000M")
      }
    }

    val sparkHiveConfigs2 = Map(
      ENGINE_SHARE_LEVEL.key -> "USER",
      ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "spark",
      "spark.driver.memory" -> "1001M")
    withSessionConf(Map.empty)(sparkHiveConfigs2)(Map.empty) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery(s"SET spark.driver.memory")
        assert(rs.next())
        mem = rs.getString(2)
        assert(mem === "1000M", "The sub-domain is same, so the engine reused")
      }
    }

    val sparkHiveConfigs3 = Map(
      ENGINE_SHARE_LEVEL.key -> "USER",
      ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "kyuubi",
      "spark.driver.memory" -> "1002M")
    withSessionConf(Map.empty)(sparkHiveConfigs3)(Map.empty) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery(s"SET spark.driver.memory")
        assert(rs.next())
        mem = rs.getString(2)
        assert(mem === "1002M", "The sub-domain is changed, so the engine recreated")
      }
    }
  }

  test("KYUUBI-1784: float types should not lose precision") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT cast(0.1 as float) AS col")
      assert(resultSet.next())
      assert(resultSet.getString("col") == "0.1")
    }
  }

  test("Spark session timezone format") {
    withJdbcStatement() { statement =>
      val setUTCResultSet = statement.executeQuery("set spark.sql.session.timeZone=UTC")
      assert(setUTCResultSet.next())
      val utcResultSet = statement.executeQuery("select from_utc_timestamp(from_unixtime(" +
        "1670404535000/1000,'yyyy-MM-dd HH:mm:ss'),'GMT+08:00')")
      assert(utcResultSet.next())
      assert(utcResultSet.getString(1) == "2022-12-07 17:15:35.0")
      val setGMT8ResultSet = statement.executeQuery("set spark.sql.session.timeZone=GMT+8")
      assert(setGMT8ResultSet.next())
      val gmt8ResultSet = statement.executeQuery("select from_utc_timestamp(from_unixtime(" +
        "1670404535000/1000,'yyyy-MM-dd HH:mm:ss'),'GMT+08:00')")
      assert(gmt8ResultSet.next())
      assert(gmt8ResultSet.getString(1) == "2022-12-08 01:15:35.0")
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl
}
