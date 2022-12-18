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

import java.nio.ByteBuffer

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TColumn, TColumnDesc, TGetResultSetMetadataResp, TPrimitiveTypeEntry, TRowSet, TStringColumn, TTableSchema, TTypeDesc, TTypeEntry, TTypeId}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.util.ThriftUtils

class NoopOperation(session: Session, shouldFail: Boolean = false)
  extends AbstractOperation(session) {
  override protected def runInternal(): Unit = {
    setState(OperationState.RUNNING)
    if (shouldFail) {
      val exception = KyuubiSQLException("noop operation err")
      setOperationException(exception)
      setState(OperationState.ERROR)
    }
    setHasResultSet(true)
  }

  override protected def beforeRun(): Unit = {
    setState(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = {
    if (!OperationState.isTerminal(state)) {
      setState(OperationState.FINISHED)
    }
  }

  override def cancel(): Unit = {
    setState(OperationState.CANCELED)

  }

  override def close(): Unit = {
    setState(OperationState.CLOSED)
  }

  override def getResultSetMetadata: TGetResultSetMetadataResp = {
    val tColumnDesc = new TColumnDesc()
    tColumnDesc.setColumnName("noop")
    val desc = new TTypeDesc
    desc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE)))
    tColumnDesc.setTypeDesc(desc)
    tColumnDesc.setComment("comment")
    tColumnDesc.setPosition(0)
    val schema = new TTableSchema()
    schema.addToColumns(tColumnDesc)
    val resp = new TGetResultSetMetadataResp
    resp.setSchema(schema)
    resp.setStatus(OK_STATUS)
    resp
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    val col = TColumn.stringVal(new TStringColumn(Seq(opType).asJava, ByteBuffer.allocate(0)))
    val tRowSet = ThriftUtils.newEmptyRowSet
    tRowSet.addToColumns(col)
    tRowSet
  }

  override def shouldRunAsync: Boolean = false

  override def getOperationLog: Option[OperationLog] = None
}
