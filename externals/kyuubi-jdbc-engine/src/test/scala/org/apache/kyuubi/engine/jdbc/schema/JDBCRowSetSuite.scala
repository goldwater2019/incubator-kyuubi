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

import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.jdbc.enumeration.JDBCColumnType
import org.apache.kyuubi.engine.jdbc.model.JDBCColumn

import java.nio.ByteBuffer
import java.sql.{Date, Time}
import scala.collection.JavaConverters._

class JDBCRowSetSuite extends KyuubiFunSuite {

  final private val UUID_PREFIX = "486bb66f-1206-49e3-993f-0db68f3cd8"

  def genRow(value: Int): List[_] = {
    val boolVal = value % 3 match {
      case 0 => true
      case 1 => false
      case _ => null
    }
    val byteVal = value.toByte
    val shortVal = value.toShort
    val longVal = value.toLong
    val charVal = String.format("%10s", value.toString)
    val floatVal = java.lang.Float.valueOf(s"$value.$value")
    val doubleVal = java.lang.Double.valueOf(s"$value.$value")
    val stringVal = value.toString * value
    val decimalVal = new java.math.BigDecimal(s"$value.$value").toPlainString
    val dayOrTime = java.lang.String.format("%02d", java.lang.Integer.valueOf(value + 1))
    val dateVal = s"2018-11-$dayOrTime"
    val timeVal = s"13:33:$dayOrTime"
    val timestampVal = s"2018-11-17 13:33:33.$value"
    // val timestampWithZoneVal = s"2018-11-17 13:33:33.$value Asia/Shanghai"
    val binaryVal = Array.fill[Byte](value)(value.toByte)
    val arrVal = Array.fill(value)(doubleVal).toList.asJava
    val mapVal = Map(value -> doubleVal).asJava
    val jsonVal = s"""{"$value": $value}"""
    val ipVal = s"${value}.${value}.${value}.${value}"
    val uuidVal = java.util.UUID.fromString(
      s"$UUID_PREFIX${uuidSuffix(value)}")

    List(
      longVal,
      value,
      shortVal,
      byteVal,
      boolVal,
      dateVal,
      decimalVal,
      floatVal,
      doubleVal,
      timestampVal,
      // timestampWithZoneVal,
      timeVal,
      binaryVal,
      stringVal,
      charVal,
      arrVal,
      mapVal,
      jsonVal,
      ipVal,
      uuidVal
    )
  }

  val schema: List[JDBCColumn] = List(
    JDBCColumn.builder().jdbcColumnName("a").jdbcColumnType(JDBCColumnType.BIGINT).build(),
    JDBCColumn.builder().jdbcColumnName("b").jdbcColumnType(JDBCColumnType.INTEGER).build(),
    JDBCColumn.builder().jdbcColumnName("c").jdbcColumnType(JDBCColumnType.SMALLINT).build(),
    JDBCColumn.builder().jdbcColumnName("d").jdbcColumnType(JDBCColumnType.TINYINT).build(),
    JDBCColumn.builder().jdbcColumnName("e").jdbcColumnType(JDBCColumnType.BOOLEAN).build(),
    JDBCColumn.builder().jdbcColumnName("f").jdbcColumnType(JDBCColumnType.DATE).build(),
    JDBCColumn.builder().jdbcColumnName("g").jdbcColumnType(JDBCColumnType.DECIMAL).build(),
    JDBCColumn.builder().jdbcColumnName("h").jdbcColumnType(JDBCColumnType.REAL).build(),
    JDBCColumn.builder().jdbcColumnName("i").jdbcColumnType(JDBCColumnType.DOUBLE).build(),
    JDBCColumn.builder().jdbcColumnName("j").jdbcColumnType(JDBCColumnType.TIMESTAMP).build(),
    JDBCColumn.builder().jdbcColumnName("k").jdbcColumnType(JDBCColumnType.TIME).build(),
    JDBCColumn.builder().jdbcColumnName("l").jdbcColumnType(JDBCColumnType.VARBINARY).build(),
    JDBCColumn.builder().jdbcColumnName("m").jdbcColumnType(JDBCColumnType.VARCHAR).build(),
    JDBCColumn.builder().jdbcColumnName("n").jdbcColumnType(JDBCColumnType.CHAR).build(),
    JDBCColumn.builder().jdbcColumnName("o").jdbcColumnType(JDBCColumnType.ARRAY).build(),
    JDBCColumn.builder().jdbcColumnName("p").jdbcColumnType(JDBCColumnType.VARCHAR).build(),
    JDBCColumn.builder().jdbcColumnName("q").jdbcColumnType(JDBCColumnType.VARCHAR).build(),
    JDBCColumn.builder().jdbcColumnName("r").jdbcColumnType(JDBCColumnType.VARCHAR).build(),
    JDBCColumn.builder().jdbcColumnName("s").jdbcColumnType(JDBCColumnType.VARCHAR).build(),
    //    JDBCColumn.builder().jdbcColumnName("t").jdbcColumnType().build(),
    //    JDBCColumn.builder().jdbcColumnName("u").jdbcColumnType().build(),
    //    JDBCColumn.builder().jdbcColumnName().jdbcColumnType().build(),
    //    JDBCColumn.builder().jdbcColumnName().jdbcColumnType().build(),
    //    JDBCColumn.builder().jdbcColumnName().jdbcColumnType().build(),
    //    JDBCColumn.builder().jdbcColumnName().jdbcColumnType().build(),
    //    JDBCColumn.builder().jdbcColumnName().jdbcColumnType().build(),
    //    JDBCColumn.builder().jdbcColumnName().jdbcColumnType().build(),
    //    JDBCColumn.builder().jdbcColumnName().jdbcColumnType().build(),
    //    JDBCColumn.builder().jdbcColumnName().jdbcColumnType().build(),
    //    JDBCColumn.builder().jdbcColumnName().jdbcColumnType().build(),

  )

  private val rows: Seq[List[_]] = (0 to 10).map(genRow) ++ Seq(List.fill(21)(null))

  def column(name: String, jdbcColumnType: JDBCColumnType): JDBCColumn = {
    JDBCColumn.builder().jdbcColumnName(name).jdbcColumnType(jdbcColumnType).build
  }

  def uuidSuffix(value: Int): String = if (value > 9) value.toString else s"f$value"

  test("column based set") {
    val tRowSet = JDBCRowSet.jdbc2ColumnBasedSet(rows, schema)
    assert(tRowSet.getColumns.size === schema.size)
    assert(tRowSet.getRowsSize === 0)

    val cols = tRowSet.getColumns.iterator()
    val longCol = cols.next().getI64Val
    longCol.getValues.asScala.zipWithIndex.foreach {
      case (value, 11) => assert(value === 0)
      case (value, index) => assert(value == index)
    }

    val intCol = cols.next().getI32Val
    intCol.getValues.asScala.zipWithIndex.foreach {
      case (value, 11) => assert(value === 0)
      case (value, index) => assert(value == index)
    }

    val shortCol = cols.next().getI16Val
    shortCol.getValues.asScala.zipWithIndex.foreach {
      case (value, 11) => assert(value === 0)
      case (value, index) => assert(value === index)
    }

    val byteCol = cols.next.getByteVal
    byteCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b === i)
    }

    val boolCol = cols.next().getBoolVal
    boolCol.getValues.asScala.zipWithIndex.foreach {
      case (b, i) =>
        i % 3 match {
          case 0 => assert(b)
          case 1 => assert(!b)
          case _ => assert(b)
        }
    }

    val dateCol = cols.next().getStringVal
    dateCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, index) =>
        val dateColumnSchema = JDBCColumn.builder().jdbcColumnType(JDBCColumnType.DATE).build()
        val date = JDBCRowSet.jdbc2HiveString(Date.valueOf(s"2018-11-${index + 1}"),
          dateColumnSchema)
        assert(b === date)
    }

    val decimalCol = cols.next().getStringVal
    decimalCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b === s"$i.$i")
    }

    val floatCol = cols.next().getDoubleVal
    floatCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b == 0)
      case (b, i) => assert(b === java.lang.Double.valueOf(s"$i.$i"))
    }

    val doubleCol = cols.next().getDoubleVal
    doubleCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b === java.lang.Double.valueOf(s"$i.$i"))
    }

    val timestampCol = cols.next().getStringVal
    timestampCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b === s"2018-11-17 13:33:33.$i")
    }

    val timeCol = cols.next().getStringVal
    timeCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) =>
        assert(b ===
        JDBCRowSet.jdbc2HiveString(Time.valueOf(s"13:33:${i + 1}"),
          JDBCColumn.builder().jdbcColumnType(JDBCColumnType.TIME).build()))
    }

    val binCol = cols.next().getBinaryVal
    binCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === ByteBuffer.allocate(0))
      case (b, i) => assert(b === ByteBuffer.wrap(Array.fill[Byte](i)(i.toByte)))
    }
  }


  test("to row set") {
    TProtocolVersion.values().foreach { proto =>
      val tRowSet = JDBCRowSet.jdbc2TRowSet(rows, schema, proto)
      if (proto.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
        assert(!tRowSet.isSetColumns, proto.toString)
        assert(tRowSet.isSetRows, proto.toString)
      } else {
        assert(tRowSet.isSetColumns, proto.toString)
        assert(tRowSet.isSetRows, proto.toString)
      }
    }
  }
}
