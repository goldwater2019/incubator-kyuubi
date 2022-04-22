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

package org.apache.kyuubi.engine.jdbc.util

import org.apache.kyuubi.engine.jdbc.enumeration.JDBCColumnType
import org.apache.kyuubi.engine.jdbc.model.JDBCColumn

/**
 * 用于类型schema的测试工具类
 */
object TestUtils {
  lazy val decimalTypeColumn: JDBCColumn = {  // decimal类型
    val column = new JDBCColumn()
    column.setJdbcColumnName("decimalCol")
    column.setJdbcColumnType(JDBCColumnType.DECIMAL)
    column
  }

  lazy val arrayTypeColumn: JDBCColumn = {  // array类型
    val column = new JDBCColumn()
    column.setJdbcColumnName("arrayCol")
    column.setJdbcColumnType(JDBCColumnType.ARRAY)
    column
  }
}
