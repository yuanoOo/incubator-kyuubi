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

package org.apache.kyuubi.engine.flink.operation

import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.operations.command._

import org.apache.kyuubi.engine.flink.FlinkEngineUtils.isFlinkVersionAtMost
import org.apache.kyuubi.engine.flink.result.ResultSetUtil
import org.apache.kyuubi.operation.{ExecutionMode, ParseMode, PhysicalMode, PlanOnlyMode, UnknownMode}
import org.apache.kyuubi.operation.PlanOnlyMode.{notSupportedModeError, unknownModeError}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

/**
 * Perform the statement parsing, analyzing or optimizing only without executing it
 */
class PlanOnlyStatement(
    session: Session,
    override val statement: String,
    mode: PlanOnlyMode) extends FlinkOperation(session) {

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  private val lineSeparator: String = System.lineSeparator()
  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(operationLog)
    super.beforeRun()
  }

  override protected def runInternal(): Unit = {
    try {
      val operation = executor.parseStatement(sessionId, statement)
      operation match {
        case setOperation: SetOperation =>
          resultSet = OperationUtils.runSetOperation(setOperation, executor, sessionId)
        case resetOperation: ResetOperation =>
          resultSet = OperationUtils.runResetOperation(resetOperation, executor, sessionId)
        case addJarOperation: AddJarOperation if isFlinkVersionAtMost("1.15") =>
          resultSet = OperationUtils.runAddJarOperation(addJarOperation, executor, sessionId)
        case removeJarOperation: RemoveJarOperation =>
          resultSet = OperationUtils.runRemoveJarOperation(removeJarOperation, executor, sessionId)
        case showJarsOperation: ShowJarsOperation if isFlinkVersionAtMost("1.15") =>
          resultSet = OperationUtils.runShowJarOperation(showJarsOperation, executor, sessionId)
        case _ => explainOperation(statement)
      }
    } catch {
      onError()
    }
  }

  private def explainOperation(statement: String): Unit = {
    val tableEnv: TableEnvironment = sessionContext.getExecutionContext.getTableEnvironment
    val explainPlans =
      tableEnv.explainSql(statement).split(s"$lineSeparator$lineSeparator")
    val operationPlan = mode match {
      case ParseMode => explainPlans(0).split(s"== Abstract Syntax Tree ==$lineSeparator")(1)
      case PhysicalMode =>
        explainPlans(1).split(s"== Optimized Physical Plan ==$lineSeparator")(1)
      case ExecutionMode =>
        explainPlans(2).split(s"== Optimized Execution Plan ==$lineSeparator")(1)
      case UnknownMode => throw unknownModeError(mode)
      case _ => throw notSupportedModeError(mode, "Flink SQL")
    }
    resultSet =
      ResultSetUtil.stringListToResultSet(
        List(operationPlan),
        "plan")
  }
}
