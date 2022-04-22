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

import org.apache.kyuubi.config.KyuubiConf.ENGINE_JDBC_CONNECTION_URL

class JDBCStatementSuite extends WithJDBCEngine {
  test("test simple query") {
    val jdbcContext = getJDBCContext
    val jdbcStatement = JDBCStatement(jdbcContext,
      kyuubiConf,
      "select 1 from aliyun"
    )
    val resultSet = jdbcStatement.execute()
    val columns = jdbcStatement.getJDBCColumns
    assert(columns.nonEmpty)
    assert(columns.size == 1)
    assert(resultSet.toIterator.hasNext)
    val list = resultSet.toIterator.next()
    assert(list === List("1"))


    val jdbcStatement2 = JDBCStatement(jdbcContext,
      kyuubiConf,
      "select * from aliyun.engine.t_click_logs limit 100,4"
    )
    val resultSet2 = jdbcStatement2.execute()
    val columns2 = jdbcStatement2.getJDBCColumns
    assert(columns2.nonEmpty)
    assert(columns2.size > 1)
    assert(resultSet2.toIterator.hasNext)
    val list2 = resultSet2.toIterator.next()
    assert(list2.nonEmpty)
  }

  test("test update session") {
    // TODO 测试切换集群
    val jdbcContext = getJDBCContext
    val jdbcStatement = JDBCStatement(jdbcContext,
      kyuubiConf,
      "select 1 from aliyun"
    )
  }

  test("test show create table") {
    val jdbcContext = getJDBCContext
    val jdbcStatement = JDBCStatement(jdbcContext,
      kyuubiConf,
      "show create table aliyun.engine.t_click_logs"
    )
    val resultSet = jdbcStatement.execute()
    val columns = jdbcStatement.getJDBCColumns
    assert(columns.nonEmpty)
  }

  override def withKyuubiConf: Map[String, String] = Map(
    ENGINE_JDBC_CONNECTION_URL.key -> gatewayUrl
  )
}
