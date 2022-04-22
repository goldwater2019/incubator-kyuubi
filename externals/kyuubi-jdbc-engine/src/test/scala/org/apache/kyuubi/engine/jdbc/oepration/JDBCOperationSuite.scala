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

package org.apache.kyuubi.engine.jdbc.oepration

import org.apache.kyuubi.engine.jdbc.WithJDBCEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class JDBCOperationSuite extends WithJDBCEngine with HiveJDBCTestHelper {
  override protected def jdbcUrl: String = getJdbcUrl

  override def withKyuubiConf: Map[String, String] =
    Map()

  test("execute statement") {
    withJdbcStatement() {
      statement =>
//        statement.execute("select 1 from aliyun")
        val resultSet = statement.executeQuery("select 1 from aliyun")
        assert(resultSet.next())
    }
  }
}
