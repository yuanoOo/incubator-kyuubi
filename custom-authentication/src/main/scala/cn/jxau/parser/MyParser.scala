package cn.jxau.parser

import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.{DataType, StructType}

class MyParser(parser: ParserInterface) extends ParserInterface {
  /**
   * Parse a string to a [[LogicalPlan]].
   */
  override def parsePlan(sqlText: String): LogicalPlan = {
    val logicalPlan = parser.parsePlan(sqlText)
    logicalPlan transform {
      case project @ Project(projectList, _) =>
        projectList.foreach {
          name =>
            if (name.isInstanceOf[UnresolvedStar]) {
              throw new RuntimeException("You must specify your project column set," +
                " * is not allowed.")
            }
        }
        project
    }
    logicalPlan
  }

  /**
   * Parse a string to an [[Expression]].
   */
  override def parseExpression(sqlText: String): Expression = parser.parseExpression(sqlText)

  /**
   * Parse a string to a [[TableIdentifier]].
   */
  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    parser.parseTableIdentifier(sqlText)

  /**
   * Parse a string to a [[FunctionIdentifier]].
   */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    parser.parseFunctionIdentifier(sqlText)

  /**
   * Parse a string to a [[StructType]]. The passed SQL string should be a comma separated
   * list of field definitions which will preserve the correct Hive metadata.
   */
  override def parseTableSchema(sqlText: String): StructType =
    parser.parseTableSchema(sqlText)

  /**
   * Parse a string to a [[DataType]].
   */
  override def parseDataType(sqlText: String): DataType = parser.parseDataType(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = parser.parseMultipartIdentifier(sqlText)
}