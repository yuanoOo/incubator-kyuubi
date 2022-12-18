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

package org.apache.kyuubi.operation

import java.sql.{DatabaseMetaData, ResultSet, SQLException, SQLFeatureNotSupportedException}

import scala.util.Random

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiSQLException}
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

// For both `in-memory` and `hive` external catalog
trait SparkMetadataTests extends HiveJDBCTestHelper {
  test("get catalogs") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      val catalogs = metaData.getCatalogs
      catalogs.next()
      assert(catalogs.getString(TABLE_CAT) === "spark_catalog")
      assert(!catalogs.next())
    }
  }

  test("get schemas") {
    val dbs = Seq("db1", "db2", "db33", "db44")
    val dbDflts = Seq("default", "global_temp")

    val catalog = "spark_catalog"
    withDatabases(dbs: _*) { statement =>
      dbs.foreach(db => statement.execute(s"CREATE DATABASE IF NOT EXISTS $db"))
      val metaData = statement.getConnection.getMetaData

      Seq("", "*", "%", null, ".*", "_*", "_%", ".%") foreach { pattern =>
        checkGetSchemas(metaData.getSchemas(catalog, pattern), dbs ++ dbDflts, catalog)
      }

      Seq("db%", "db.*") foreach { pattern =>
        checkGetSchemas(metaData.getSchemas(catalog, pattern), dbs, catalog)
      }

      Seq("db_", "db.") foreach { pattern =>
        checkGetSchemas(metaData.getSchemas(catalog, pattern), dbs.take(2), catalog)
      }

      checkGetSchemas(metaData.getSchemas(catalog, "db1"), Seq("db1"), catalog)
      checkGetSchemas(metaData.getSchemas(catalog, "db_not_exist"), Seq.empty, catalog)

      checkGetSchemas(metaData.getSchemas(catalog, "global\\_temp"), Seq("global_temp"), catalog)
    }
  }

  test("get tables") {
    val table_test = "table_1_test"
    val view_test = "view_1_test"
    val view_global_test = "view_2_test"
    val tables = Seq(table_test, view_test, view_global_test)
    val schemas = Seq("default", "default", "global_temp")
    val tableTypes = Seq("TABLE", "VIEW", "VIEW")
    withJdbcStatement(view_test, view_global_test, table_test, view_test) { statement =>
      statement.execute(
        s"CREATE TABLE IF NOT EXISTS $table_test(key int) USING parquet COMMENT '$table_test'")
      statement.execute(s"CREATE VIEW IF NOT EXISTS $view_test COMMENT '$view_test'" +
        s" AS SELECT * FROM $table_test")
      statement.execute(s"CREATE GLOBAL TEMP VIEW $view_global_test" +
        s" COMMENT '$view_global_test' AS SELECT * FROM $table_test")

      val metaData = statement.getConnection.getMetaData
      val rs1 = metaData.getTables(null, null, null, null)
      var i = 0
      while (rs1.next()) {
        val catalogName = rs1.getString(TABLE_CAT)
        assert(catalogName === "spark_catalog" || catalogName === null)
        assert(rs1.getString(TABLE_SCHEM) === schemas(i))
        assert(rs1.getString(TABLE_NAME) == tables(i))
        assert(rs1.getString(TABLE_TYPE) == tableTypes(i))
        assert(rs1.getString(REMARKS) === tables(i).replace(view_global_test, ""))
        i += 1
      }
      assert(i === 3)

      val rs2 = metaData.getTables(null, null, null, Array("VIEW"))
      i = 1
      while (rs2.next()) {
        assert(rs2.getString(TABLE_NAME) == tables(i))
        i += 1
      }
      assert(i === 3)

      val rs3 = metaData.getTables(null, "*", "*", Array("VIEW"))
      i = 1
      while (rs3.next()) {
        assert(rs3.getString(TABLE_NAME) == tables(i))
        i += 1
      }
      assert(i === 3)

      val rs4 = metaData.getTables(null, null, "table%", Array("VIEW"))
      assert(!rs4.next())

      val rs5 = metaData.getTables(null, "*", "table%", Array("VIEW"))
      assert(!rs5.next())

      val rs6 = metaData.getTables(null, null, "table%", Array("TABLE"))
      i = 0
      while (rs6.next()) {
        assert(rs6.getString(TABLE_NAME) == tables(i))
        i += 1
      }
      assert(i === 1)

      val rs7 = metaData.getTables(null, "default", "%", Array("VIEW"))
      i = 1
      while (rs7.next()) {
        assert(rs7.getString(TABLE_NAME) == view_test)
      }

      statement.execute(s"DROP TABLE IF EXISTS ${schemas(0)}.$table_test")
      statement.execute(s"DROP VIEW IF EXISTS ${schemas(1)}.$view_test")
      statement.execute(s"DROP VIEW IF EXISTS ${schemas(2)}.$view_global_test")
    }
  }

  test("get type info") {
    withJdbcStatement() { statement =>
      val typeInfo = statement.getConnection.getMetaData.getTypeInfo
      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "VOID")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.NULL)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "BOOLEAN")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BOOLEAN)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "TINYINT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.TINYINT)
      assert(typeInfo.getInt(PRECISION) === 3)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "SMALLINT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.SMALLINT)
      assert(typeInfo.getInt(PRECISION) === 5)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "INTEGER")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.INTEGER)
      assert(typeInfo.getInt(PRECISION) === 10)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "BIGINT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BIGINT)
      assert(typeInfo.getInt(PRECISION) === 19)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "FLOAT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.FLOAT)
      assert(typeInfo.getInt(PRECISION) === 7)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "DOUBLE")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DOUBLE)
      assert(typeInfo.getInt(PRECISION) === 15)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "STRING")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.VARCHAR)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "BINARY")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BINARY)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "DECIMAL")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DECIMAL)
      assert(typeInfo.getInt(PRECISION) === 38)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "DATE")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DATE)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "TIMESTAMP")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.TIMESTAMP)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "ARRAY")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.ARRAY)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "MAP")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.JAVA_OBJECT)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "STRUCT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.STRUCT)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "INTERVAL")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)
    }
  }

  test("audit Kyuubi Hive JDBC connection common MetaData") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      Seq(
        () => metaData.allProceduresAreCallable(),
        () => metaData.getURL,
        () => metaData.getUserName,
        () => metaData.isReadOnly,
        () => metaData.nullsAreSortedHigh,
        () => metaData.nullsAreSortedLow,
        () => metaData.nullsAreSortedAtStart(),
        () => metaData.nullsAreSortedAtEnd(),
        () => metaData.usesLocalFiles(),
        () => metaData.usesLocalFilePerTable(),
        () => metaData.supportsMixedCaseIdentifiers(),
        () => metaData.supportsMixedCaseQuotedIdentifiers(),
        () => metaData.storesUpperCaseIdentifiers(),
        () => metaData.storesUpperCaseQuotedIdentifiers(),
        () => metaData.storesLowerCaseIdentifiers(),
        () => metaData.storesLowerCaseQuotedIdentifiers(),
        () => metaData.storesMixedCaseIdentifiers(),
        () => metaData.storesMixedCaseQuotedIdentifiers(),
        () => metaData.nullPlusNonNullIsNull,
        () => metaData.supportsConvert,
        () => metaData.supportsTableCorrelationNames,
        () => metaData.supportsDifferentTableCorrelationNames,
        () => metaData.supportsExpressionsInOrderBy(),
        () => metaData.supportsOrderByUnrelated,
        () => metaData.supportsGroupByUnrelated,
        () => metaData.supportsGroupByBeyondSelect,
        () => metaData.supportsLikeEscapeClause,
        () => metaData.supportsMultipleTransactions,
        () => metaData.supportsMinimumSQLGrammar,
        () => metaData.supportsCoreSQLGrammar,
        () => metaData.supportsExtendedSQLGrammar,
        () => metaData.supportsANSI92EntryLevelSQL,
        () => metaData.supportsANSI92IntermediateSQL,
        () => metaData.supportsANSI92FullSQL,
        () => metaData.supportsIntegrityEnhancementFacility,
        () => metaData.isCatalogAtStart,
        () => metaData.supportsSubqueriesInComparisons,
        () => metaData.supportsSubqueriesInExists,
        () => metaData.supportsSubqueriesInIns,
        () => metaData.supportsSubqueriesInQuantifieds,
        // Spark support this, see https://issues.apache.org/jira/browse/SPARK-18455
        () => metaData.supportsCorrelatedSubqueries,
        () => metaData.supportsOpenCursorsAcrossCommit,
        () => metaData.supportsOpenCursorsAcrossRollback,
        () => metaData.supportsOpenStatementsAcrossCommit,
        () => metaData.supportsOpenStatementsAcrossRollback,
        () => metaData.getMaxBinaryLiteralLength,
        () => metaData.getMaxCharLiteralLength,
        () => metaData.getMaxColumnsInGroupBy,
        () => metaData.getMaxColumnsInIndex,
        () => metaData.getMaxColumnsInOrderBy,
        () => metaData.getMaxColumnsInSelect,
        () => metaData.getMaxColumnsInTable,
        () => metaData.getMaxConnections,
        () => metaData.getMaxCursorNameLength,
        () => metaData.getMaxIndexLength,
        () => metaData.getMaxSchemaNameLength,
        () => metaData.getMaxProcedureNameLength,
        () => metaData.getMaxCatalogNameLength,
        () => metaData.getMaxRowSize,
        () => metaData.doesMaxRowSizeIncludeBlobs,
        () => metaData.getMaxStatementLength,
        () => metaData.getMaxStatements,
        () => metaData.getMaxTableNameLength,
        () => metaData.getMaxTablesInSelect,
        () => metaData.getMaxUserNameLength,
        () => metaData.supportsTransactionIsolationLevel(1),
        () => metaData.supportsDataDefinitionAndDataManipulationTransactions,
        () => metaData.supportsDataManipulationTransactionsOnly,
        () => metaData.dataDefinitionCausesTransactionCommit,
        () => metaData.dataDefinitionIgnoredInTransactions,
        () => metaData.getColumnPrivileges("", "%", "%", "%"),
        () => metaData.getTablePrivileges("", "%", "%"),
        () => metaData.getBestRowIdentifier("", "%", "%", 0, true),
        () => metaData.getVersionColumns("", "%", "%"),
        () => metaData.getExportedKeys("", "default", ""),
        () => metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, 2),
        () => metaData.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.supportsNamedParameters(),
        () => metaData.supportsMultipleOpenResults,
        () => metaData.supportsGetGeneratedKeys,
        () => metaData.getSuperTypes("", "%", "%"),
        () => metaData.getSuperTables("", "%", "%"),
        () => metaData.getAttributes("", "%", "%", "%"),
        () => metaData.getResultSetHoldability,
        () => metaData.locatorsUpdateCopy,
        () => metaData.supportsStatementPooling,
        () => metaData.getRowIdLifetime,
        () => metaData.supportsStoredFunctionsUsingCallSyntax,
        () => metaData.autoCommitFailureClosesAllResultSets,
        () => metaData.getFunctionColumns("", "%", "%", "%"),
        () => metaData.getPseudoColumns("", "%", "%", "%"),
        () => metaData.generatedKeyAlwaysReturned).foreach { func =>
        val e = intercept[SQLFeatureNotSupportedException](func())
        assert(e.getMessage === "Method not supported")
      }

      assert(metaData.allTablesAreSelectable)
      assert(metaData.getClientInfoProperties.next)
      assert(metaData.getDriverName === "Kyuubi Project Hive JDBC Client" ||
        metaData.getDriverName === "Kyuubi Project Hive JDBC Shaded Client")
      assert(metaData.getDriverVersion === KYUUBI_VERSION)
      assert(
        metaData.getIdentifierQuoteString === " ",
        "This method returns a space \" \" if identifier quoting is not supported")
      assert(metaData.getNumericFunctions === "")
      assert(metaData.getStringFunctions === "")
      assert(metaData.getSystemFunctions === "")
      assert(metaData.getTimeDateFunctions === "")
      assert(metaData.getSearchStringEscape === "\\")
      assert(metaData.getExtraNameCharacters === "")
      assert(metaData.supportsAlterTableWithAddColumn())
      assert(!metaData.supportsAlterTableWithDropColumn())
      assert(metaData.supportsColumnAliasing())
      assert(metaData.supportsGroupBy)
      assert(!metaData.supportsMultipleResultSets)
      assert(!metaData.supportsNonNullableColumns)
      assert(metaData.supportsOuterJoins)
      assert(metaData.supportsFullOuterJoins)
      assert(metaData.supportsLimitedOuterJoins)
      assert(metaData.getSchemaTerm === "database")
      assert(metaData.getProcedureTerm === "UDF")
      assert(metaData.getCatalogTerm === "catalog")
      assert(metaData.getCatalogSeparator === ".")
      assert(metaData.supportsSchemasInDataManipulation)
      assert(!metaData.supportsSchemasInProcedureCalls)
      assert(metaData.supportsSchemasInTableDefinitions)
      assert(!metaData.supportsSchemasInIndexDefinitions)
      assert(!metaData.supportsSchemasInPrivilegeDefinitions)
      assert(metaData.supportsCatalogsInDataManipulation)
      assert(metaData.supportsCatalogsInProcedureCalls)
      assert(metaData.supportsCatalogsInTableDefinitions)
      assert(metaData.supportsCatalogsInIndexDefinitions)
      assert(metaData.supportsCatalogsInPrivilegeDefinitions)
      assert(!metaData.supportsPositionedDelete)
      assert(!metaData.supportsPositionedUpdate)
      assert(!metaData.supportsSelectForUpdate)
      assert(!metaData.supportsStoredProcedures)
      // This is actually supported, but hive jdbc package return false
      assert(!metaData.supportsUnion)
      assert(metaData.supportsUnionAll)
      assert(metaData.getMaxColumnNameLength === 128)
      assert(metaData.getDefaultTransactionIsolation === java.sql.Connection.TRANSACTION_NONE)
      assert(!metaData.supportsTransactions)
      assert(!metaData.getProcedureColumns("", "%", "%", "%").next())
      val e1 = intercept[SQLException] {
        metaData.getPrimaryKeys("", "default", "src").next()
      }
      assert(e1.getMessage.contains(KyuubiSQLException.featureNotSupported().getMessage))
      assert(!metaData.getImportedKeys("", "default", "").next())

      val e2 = intercept[SQLException] {
        metaData.getCrossReference("", "default", "src", "", "default", "src2").next()
      }
      assert(e2.getMessage.contains(KyuubiSQLException.featureNotSupported().getMessage))
      assert(!metaData.getIndexInfo("", "default", "src", true, true).next())

      assert(metaData.supportsResultSetType(new Random().nextInt()))
      assert(!metaData.supportsBatchUpdates)
      assert(!metaData.getUDTs(",", "%", "%", null).next())
      assert(!metaData.supportsSavepoints)
      assert(!metaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT))
      assert(metaData.getJDBCMajorVersion === 3)
      assert(metaData.getJDBCMinorVersion === 0)
      assert(metaData.getSQLStateType === DatabaseMetaData.sqlStateSQL)
      assert(metaData.getMaxLogicalLobSize === 0)
      assert(!metaData.supportsRefCursors)
    }
  }
}
