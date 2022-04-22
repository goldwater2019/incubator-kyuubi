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

package org.apache.kyuubi.engine.jdbc.operation

import java.io.IOException

import org.apache.hive.service.rpc.thrift.{TRowSet, TTableSchema}

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.engine.jdbc.JDBCContext
import org.apache.kyuubi.engine.jdbc.client.JDBCEngineGatewayClientManager
import org.apache.kyuubi.engine.jdbc.model.JDBCColumn
import org.apache.kyuubi.engine.jdbc.schema.{JDBCRowSet, JDBCSchemaHelper}
import org.apache.kyuubi.engine.jdbc.session.JDBCSessionImpl
import org.apache.kyuubi.operation.{AbstractOperation, FetchIterator, OperationState}
import org.apache.kyuubi.operation.FetchOrientation.{FETCH_FIRST, FETCH_NEXT, FETCH_PRIOR, FetchOrientation}
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.OperationType.OperationType
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

abstract class JDBCOperation(opType: OperationType, session: Session)
  extends AbstractOperation(opType, session) {

  protected val jdbcContext: JDBCContext = session.asInstanceOf[JDBCSessionImpl].jdbcContext

  // protected var trino: StatementClient = _
  protected var jdbc: JDBCEngineGatewayClientManager = _;

  protected var schema: List[JDBCColumn] = _

  protected var iter: FetchIterator[List[Any]] = _

  override def getResultSetSchema: TTableSchema = JDBCSchemaHelper.jdbc2TTableSchema(schema)

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    order match {
      case FETCH_NEXT => iter.fetchNext()
      case FETCH_PRIOR => iter.fetchPrior(rowSetSize);
      case FETCH_FIRST => iter.fetchAbsolute(0);
    }
    val taken = iter.take(rowSetSize)
    val resultRowSet = JDBCRowSet.jdbc2TRowSet(taken.toList, schema, getProtocolVersion)
    resultRowSet.setStartRowOffset(iter.getPosition)
    resultRowSet
  }

  override protected def beforeRun(): Unit = {
    setHasResultSet(true)
    setState(OperationState.RUNNING)
  }

  override protected def afterRun(): Unit = {
    state.synchronized {
      if (!isTerminalState(state)) {
        setState(OperationState.FINISHED)
      }
    }
    OperationLog.removeCurrentOperationLog()
  }

  override def cancel(): Unit = {
    cleanup(OperationState.CANCELED)
  }

  protected def cleanup(targetState: OperationState): Unit = state.synchronized {
    if (!isTerminalState(state)) {
      setState(targetState)
      Option(getBackgroundHandle).foreach(_.cancel(true))
    }
  }

  override def close(): Unit = {
    cleanup(OperationState.CLOSED)
    try {
      getOperationLog.foreach(_.close())
    } catch {
      case e: IOException =>
        error(e.getMessage, e)
    }
  }

  override def shouldRunAsync: Boolean = false

  protected def onError(cancel: Boolean = false): PartialFunction[Throwable, Unit] = {
    // We should use Throwable instead of Exception since `java.lang.NoClassDefFoundError`
    // could be thrown.
    case e: Throwable =>
      state.synchronized {
        val errMsg = Utils.stringifyException(e)
        if (state == OperationState.TIMEOUT) {
          val ke = KyuubiSQLException(s"Timeout operating $opType: $errMsg")
          setOperationException(ke)
          throw ke
        } else if (isTerminalState(state)) {
          setOperationException(KyuubiSQLException(errMsg))
          warn(s"Ignore exception in terminal state with $statementId: $errMsg")
        } else {
          error(s"Error operating $opType: $errMsg", e)
          val ke = KyuubiSQLException(s"Error operating $opType: $errMsg", e)
          setOperationException(ke)
          setState(OperationState.ERROR)
          throw ke
        }
      }
  }
}
