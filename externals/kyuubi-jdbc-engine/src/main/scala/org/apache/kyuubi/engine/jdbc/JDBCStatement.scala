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

package org.apache.kyuubi.engine.jdbc

import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_JDBC_CONNECTION_URL
import org.apache.kyuubi.engine.jdbc.client.JDBCEngineGatewayClientManager
import org.apache.kyuubi.engine.jdbc.enumeration.JDBCQueryStatus
import org.apache.kyuubi.engine.jdbc.model.{JDBCColumn, JDBCResultRef}

/**
 * jdbc client communicate with jdbc cluster.
 */
class JDBCStatement(jdncContext: JDBCContext, kyuubiConf: KyuubiConf, sql: String) {

  private lazy val jdbc = JDBCEngineGatewayClientManager.getInstance()

  def getJDBCClient: JDBCEngineGatewayClientManager = jdbc

  def getCurrentDatabase: String = "default";
  // trinoContext.clientSession.get.getSchema
  // TODO 此处是有必要仍然保留 currentDatabase 的获取

  var jdbcResultRef: JDBCResultRef = _

  def getJDBCColumns: List[JDBCColumn] = {
    if (jdbcResultRef == null) {
      throw KyuubiSQLException(
        s"invalid query or query is not executed before, sql: $sql")
    } else if (jdbcResultRef.getJdbcOperationRef.getQueryStatus != JDBCQueryStatus.OK) {
      throw KyuubiSQLException(
        s"Query failed (#${jdbcResultRef.getJdbcOperationRef.getOperationRefId}):" +
          s" ${jdbcResultRef.getJdbcOperationRef.getMessage}")
    }
    jdbcResultRef.getJdbcColumnList.asScala.toList
  }

  var baseUrl: String = kyuubiConf.get(ENGINE_JDBC_CONNECTION_URL)
    .getOrElse(throw KyuubiSQLException("no jdbc engine url specified, " +
      "please specify it by session.engine.jdbc.connection.url"))

  /**
   * Execute sql and return ResultSet.
   */
  def execute(): Iterable[List[Any]] = {
    val result = ArrayBuffer[List[Any]]()
    val client = getJDBCClient
    jdbcResultRef = client.querySql(sql, baseUrl).getData
    val rowSetList = jdbcResultRef.getJdbcRowSetList
    for (elem <- rowSetList.asScala) {
      result += elem
    }
    result
  }
}

object JDBCStatement {
  // final private val MAX_QUEUED_ROWS = 50000
  // final private val MAX_BUFFERED_ROWS = 10000
  // final private val MAX_BUFFER_TIME = Duration(3, duration.SECONDS)
  // final private val END_TOKEN = List[Any]()
  def apply(jdbcContext: JDBCContext, kyuubiConf: KyuubiConf, sql: String): JDBCStatement = {
    new JDBCStatement(jdbcContext, kyuubiConf, sql)
  }
}
