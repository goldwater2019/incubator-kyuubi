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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.jdbc.JDBCStatement
import org.apache.kyuubi.operation.{ArrayFetchIterator, IterableFetchIterator, OperationState, OperationType}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

class JDBCExecuteStatement(
    session: Session,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    var incrementalCollect: Boolean)
  extends JDBCOperation(OperationType.EXECUTE_STATEMENT, session) with Logging {

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(operationLog)
    setState(OperationState.PENDING)
    setHasResultSet(true)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  override protected def runInternal(): Unit = {
    val jdbcStatement = JDBCStatement(jdbcContext, session.sessionManager.getConf, statement)
    jdbc = jdbcStatement.getJDBCClient
    executeStatement(jdbcStatement)
//    if (shouldRunAsync) {
//      val asyncOperation = new Runnable {
//        override def run(): Unit = {
//          OperationLog.setCurrentOperationLog(operationLog)
//          executeStatement(trinoStatement)
//        }
//      }
//
//      try {
//        val trinoSessionManager = session.sessionManager
//        val backgroundHandle = trinoSessionManager.submitBackgroundOperation(asyncOperation)
//        setBackgroundHandle(backgroundHandle)
//      } catch {
//        case rejected: RejectedExecutionException =>
//          setState(OperationState.ERROR)
//          val ke =
//            KyuubiSQLException("Error submitting query in background, query rejected", rejected)
//          setOperationException(ke)
//          throw ke
//      }
//    } else {
//      executeStatement(trinoStatement)
//    }
  }

  private def executeStatement(jdbcStatement: JDBCStatement): Unit = {
    setState(OperationState.RUNNING)
    try {
      schema = jdbcStatement.getJDBCColumns
      val resultSet = jdbcStatement.execute()
      incrementalCollect = false
      iter =
        if (incrementalCollect) {
          info("Execute in incremental collect mode")
          new IterableFetchIterator(resultSet)
        } else {
          info("Execute in full collect mode")
          new ArrayFetchIterator(resultSet.toArray)
        }
      setState(OperationState.FINISHED)
    } catch {
      onError(cancel = true)
    }
  }
}
