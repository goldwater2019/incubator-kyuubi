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

package org.apache.kyuubi.engine.jdbc.schema

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TCLIServiceConstants, TTypeId}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.jdbc.enumeration.JDBCColumnType
import org.apache.kyuubi.engine.jdbc.model.JDBCColumn
import org.apache.kyuubi.engine.jdbc.schema.JDBCSchemaHelper.{jdbc2TTableSchema, jdbc2TTypeId, jdbc2TTypeQualifiers}

class JDBCSchemaHelperSuite extends KyuubiFunSuite {
  val outerSchema: Seq[JDBCColumn] = List(
    JDBCColumn.builder().jdbcColumnName("c0").jdbcColumnType(JDBCColumnType.BOOLEAN).build(),
    JDBCColumn.builder().jdbcColumnName("c1").jdbcColumnType(JDBCColumnType.TINYINT).build(),
    JDBCColumn.builder().jdbcColumnName("c2").jdbcColumnType(JDBCColumnType.SMALLINT).build(),
    JDBCColumn.builder().jdbcColumnName("c3").jdbcColumnType(JDBCColumnType.INTEGER).build(),
    JDBCColumn.builder().jdbcColumnName("c4").jdbcColumnType(JDBCColumnType.BIGINT).build(),
    JDBCColumn.builder().jdbcColumnName("c5").jdbcColumnType(JDBCColumnType.REAL).build(),
    JDBCColumn.builder().jdbcColumnName("c6").jdbcColumnType(JDBCColumnType.DOUBLE).build(),
    JDBCColumn.builder().jdbcColumnName("c7").jdbcColumnType(JDBCColumnType.DECIMAL).build(),
    JDBCColumn.builder().jdbcColumnName("c8").jdbcColumnType(JDBCColumnType.CHAR).build(),
    JDBCColumn.builder().jdbcColumnName("c9").jdbcColumnType(JDBCColumnType.VARCHAR).build(),
    JDBCColumn.builder().jdbcColumnName("c10").jdbcColumnType(JDBCColumnType.VARBINARY).build(),
    JDBCColumn.builder().jdbcColumnName("c11").jdbcColumnType(JDBCColumnType.DATE).build(),
    JDBCColumn.builder().jdbcColumnName("c12").jdbcColumnType(JDBCColumnType.TIMESTAMP).build(),
    JDBCColumn.builder().jdbcColumnName("c13").jdbcColumnType(JDBCColumnType.ARRAY).build(),
    JDBCColumn.builder().jdbcColumnName("c14").jdbcColumnType(JDBCColumnType.BIT).build(),
    JDBCColumn.builder().jdbcColumnName("c15").jdbcColumnType(JDBCColumnType.NUMERIC).build(),
    JDBCColumn.builder().jdbcColumnName("c16").jdbcColumnType(JDBCColumnType.LONGVARCHAR).build(),
    JDBCColumn.builder().jdbcColumnName("c17").jdbcColumnType(JDBCColumnType.TIME).build(),
    JDBCColumn.builder().jdbcColumnName("c18").jdbcColumnType(JDBCColumnType.BINARY).build(),
    JDBCColumn.builder().jdbcColumnName("c19").jdbcColumnType(JDBCColumnType.LONGVARBINARY).build(),
    //    JDBCColumn.builder().jdbcColumnName("c20").jdbcColumnType(JDBCColumnType.VARCHAR).build(),
    //    JDBCColumn.builder().jdbcColumnName("c21").jdbcColumnType(JDBCColumnType.VARCHAR).build()
  )

  test("toTTypeId") {
    assert(jdbc2TTypeId(outerSchema.head) === TTypeId.BOOLEAN_TYPE)
    assert(jdbc2TTypeId(outerSchema(1)) === TTypeId.TINYINT_TYPE)
    assert(jdbc2TTypeId(outerSchema(2)) === TTypeId.SMALLINT_TYPE)
    assert(jdbc2TTypeId(outerSchema(3)) === TTypeId.INT_TYPE)
    assert(jdbc2TTypeId(outerSchema(4)) === TTypeId.BIGINT_TYPE)
    assert(jdbc2TTypeId(outerSchema(5)) === TTypeId.FLOAT_TYPE)
    assert(jdbc2TTypeId(outerSchema(6)) === TTypeId.DOUBLE_TYPE)
    assert(jdbc2TTypeId(outerSchema(7)) === TTypeId.DECIMAL_TYPE)
    assert(jdbc2TTypeId(outerSchema(8)) === TTypeId.CHAR_TYPE)
    assert(jdbc2TTypeId(outerSchema(9)) === TTypeId.STRING_TYPE)
    assert(jdbc2TTypeId(outerSchema(10)) === TTypeId.BINARY_TYPE)
    assert(jdbc2TTypeId(outerSchema(11)) === TTypeId.DATE_TYPE)
    assert(jdbc2TTypeId(outerSchema(12)) === TTypeId.TIMESTAMP_TYPE)
    assert(jdbc2TTypeId(outerSchema(13)) === TTypeId.ARRAY_TYPE)
    assert(jdbc2TTypeId(outerSchema(14)) === TTypeId.TINYINT_TYPE) // TODO
    assert(jdbc2TTypeId(outerSchema(15)) === TTypeId.DECIMAL_TYPE)
    assert(jdbc2TTypeId(outerSchema(16)) === TTypeId.STRING_TYPE)
    assert(jdbc2TTypeId(outerSchema(17)) === TTypeId.STRING_TYPE)
    assert(jdbc2TTypeId(outerSchema(18)) === TTypeId.BINARY_TYPE)
    assert(jdbc2TTypeId(outerSchema(19)) === TTypeId.BINARY_TYPE)
  }

  test("toTTypeQualifiers") {
    val qualifiers = jdbc2TTypeQualifiers(outerSchema(7))
    val q = qualifiers.getQualifiers
    assert(q.size === 2)
    val precision = q.get(TCLIServiceConstants.PRECISION).getI32Value
    val scale = q.get(TCLIServiceConstants.SCALE).getI32Value
    assert(precision === 32)
    assert(scale === 3)
  }

  test("toTTableSchema") {
    val tTableSchema = jdbc2TTableSchema(outerSchema)
    assert(tTableSchema.getColumnsSize === outerSchema.size)
    val iter = tTableSchema.getColumns
    iter.asScala.zipWithIndex.foreach { case (col, pos) =>
      val field = outerSchema(pos)
      assert(col.getColumnName === field.getJdbcColumnName)
      assert(col.getPosition === pos)
      val qualifiers =
        col.getTypeDesc.getTypes.get(0).getPrimitiveEntry.getTypeQualifiers.getQualifiers
      if (pos == 7 || pos == 15) {
        assert(qualifiers.get(TCLIServiceConstants.PRECISION).getI32Value === 32)
        assert(qualifiers.get(TCLIServiceConstants.SCALE).getI32Value === 3)
      } else {
        assert(qualifiers.isEmpty)
      }
    }
  }
}
