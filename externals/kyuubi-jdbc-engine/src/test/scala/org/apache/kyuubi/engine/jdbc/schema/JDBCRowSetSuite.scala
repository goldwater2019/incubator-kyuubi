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

import java.nio.ByteBuffer
import java.sql.{Date, Time}
import scala.collection.JavaConverters._
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.jdbc.enumeration.JDBCColumnType
import org.apache.kyuubi.engine.jdbc.model.JDBCColumn

import java.nio.charset.StandardCharsets


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
    JDBCColumn.builder().jdbcColumnName("p").jdbcColumnType(JDBCColumnType.MAP).build(),
    JDBCColumn.builder().jdbcColumnName("q").jdbcColumnType(JDBCColumnType.JSON).build(),
    JDBCColumn.builder().jdbcColumnName("r").jdbcColumnType(JDBCColumnType.IPADDRESS).build(),
    JDBCColumn.builder().jdbcColumnName("s").jdbcColumnType(JDBCColumnType.UUID).build(),
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

  private val rows: Seq[List[_]] = (0 to 10).map(genRow) ++ Seq(List.fill(schema.size)(null))

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

    val strCol = cols.next.getStringVal
    strCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b === i.toString * i)
    }

    val charCol = cols.next().getStringVal
    charCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) =>
        assert(b === String.format("%10s", i.toString))
    }

    val arrCol = cols.next().getStringVal
    arrCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) =>
        val doubles = Array.fill(i)(java.lang.Double.valueOf(s"$i.$i")).toList.asJava
        val doubleArrString = JDBCRowSet.jdbc2HiveString(doubles,
          JDBCColumn.builder().jdbcColumnType(JDBCColumnType.ARRAY).build())
        assert(b === doubleArrString)
    }

    val mapCol = cols.next().getStringVal
    mapCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === "")
      case (b, i) =>
        val mapData = Map(i -> java.lang.Double.valueOf(s"$i.$i")).asJava
        val mapHiveString = JDBCRowSet.jdbc2HiveString(mapData, JDBCColumn.builder()
          .jdbcColumnType(JDBCColumnType.MAP)
          .build())
        assert(b === mapHiveString)
    }

    val jsonCol = cols.next.getStringVal
    jsonCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) =>
        val jsonStr = s"""{"$i": $i}"""
        var jsonHiveString = JDBCRowSet.jdbc2HiveString(jsonStr,
          JDBCColumn.builder.jdbcColumnType(JDBCColumnType.JSON).build()
        )
        // jsonHiveString = jsonHiveString.substring(1, jsonHiveString.length - 1)
        assert(b === jsonHiveString)
    }

    val ipCol = cols.next.getStringVal
    ipCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b ===
      JDBCRowSet.jdbc2HiveString(
        s"$i.$i.$i.$i",
        JDBCColumn.builder().jdbcColumnType(JDBCColumnType.IPADDRESS).build()
      ))
    }

    val uuidCol = cols.next.getStringVal
    uuidCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b ===
        JDBCRowSet.jdbc2HiveString(
          java.util.UUID.fromString(
            s"$UUID_PREFIX${uuidSuffix(i)}"),
          JDBCColumn.builder().jdbcColumnType(JDBCColumnType.IPADDRESS).build()
        ))
    }
  }

  test("row based set") {
    val tRowSet = JDBCRowSet.jdbc2RowBasedSet(rows, schema)
    assert(tRowSet.getColumnCount === 0)
    assert(tRowSet.getRowsSize === rows.size)
    val iter = tRowSet.getRowsIterator

    val r1 = iter.next().getColVals
    assert(r1.get(0).getI64Val.getValue === 0)
    assert(r1.get(4).getBoolVal.isValue)

    val r2 = iter.next().getColVals
    assert(r2.get(1).getI32Val.getValue === 1)
    assert(!r2.get(4).getBoolVal.isValue)

    val r3 = iter.next().getColVals
    assert(r3.get(2).getI16Val.getValue == 2)
    assert(!r3.get(4).getBoolVal.isValue)

    val r4 = iter.next().getColVals
    assert(r4.get(3).getByteVal.getValue == 3)

    val r5 = iter.next().getColVals
    assert(r5.get(5).getStringVal.getValue === "2018-11-05")
    assert(r5.get(6).getStringVal.getValue === "4.4")

    val r6 = iter.next().getColVals
    assert(r6.get(7).getDoubleVal.getValue === 5.5)
    assert(r6.get(8).getDoubleVal.getValue === 5.5)

    val r7 = iter.next().getColVals
    assert(r7.get(9).getStringVal.getValue === "2018-11-17 13:33:33.6")
    // assert(r7.get(10).getStringVal.getValue === "2018-11-17 13:33:33.6 Asia/Shanghai")

    val r8 = iter.next().getColVals
    assert(r8.get(10).getStringVal.getValue === "13:33:08")

    val r9 = iter.next().getColVals
    assert(r9.get(11).getStringVal.getValue === new String(
      Array.fill[Byte](8)(8.toByte),
      StandardCharsets.UTF_8))
    assert(r9.get(12).getStringVal.getValue === "8" * 8)
    assert(r9.get(13).getStringVal.getValue === String.format(s"%10s", 8.toString))

    val r10 = iter.next().getColVals
    val mapStr =
      Map(9 -> 9.9d).map { case (key, value) => s"$key:$value" }.toSeq.mkString("{", ",", "}")
//    assert(r10.get(15).getStringVal.getValue ===
//      String.format("{foo=\"%s\",bar=%s}", "9", mapStr))
    assert(r10.get(14).getStringVal.getValue === Array.fill(9)(9.9d).mkString("[", ",", "]"))
    assert(r10.get(15).getStringVal.getValue === mapStr)
    assert(r10.get(16).getStringVal.getValue === "{\"9\": 9}")
    assert(r10.get(17).getStringVal.getValue === "9.9.9.9")
    assert(r10.get(18).getStringVal.getValue === s"$UUID_PREFIX${uuidSuffix(9)}")

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
