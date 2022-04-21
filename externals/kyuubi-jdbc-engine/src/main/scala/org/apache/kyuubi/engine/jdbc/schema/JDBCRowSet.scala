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
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.engine.jdbc.enumeration.JDBCColumnType
import org.apache.kyuubi.engine.jdbc.model.JDBCColumn
import org.apache.kyuubi.engine.trino.util.PreconditionsWrapper._
import org.apache.kyuubi.util.RowSetUtils.bitSetToBuffer

object JDBCRowSet {

  def jdbc2TRowSet(
      rows: Seq[List[_]],
      schema: List[JDBCColumn],
      protocolVersion: TProtocolVersion): TRowSet = {
    if (protocolVersion.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      jdbc2RowBasedSet(rows, schema)
    } else {
      jdbc2ColumnBasedSet(rows, schema)
    }
  }

  def jdbc2RowBasedSet(rows: Seq[List[_]], schema: List[JDBCColumn]): TRowSet = {
    val tRows = rows.map { row =>
      val tRow = new TRow()
      (0 until row.size).map(i => jdbc2TColumnValue(i, row, schema))
        .foreach(tRow.addToColVals)
      tRow
    }.asJava
    new TRowSet(0, tRows)
  }

  def jdbc2ColumnBasedSet(rows: Seq[List[_]], schema: List[JDBCColumn]): TRowSet = {
    val size = rows.size
    val tRowSet = new TRowSet(0, new java.util.ArrayList[TRow](size))
    schema.zipWithIndex.foreach { case (filed, i) =>
      val tColumn = jdbc2TColumn(
        rows,
        i,
        filed)
      tRowSet.addToColumns(tColumn)
    }
    tRowSet
  }

  private def jdbc2TColumn(
      rows: Seq[Seq[Any]],
      ordinal: Int,
      jdbcColumn: JDBCColumn): TColumn = {
    val nulls = new java.util.BitSet()
    jdbcColumn.getJdbcColumnType match {
      case JDBCColumnType.BOOLEAN =>
        val values = getOrSetAsNull[java.lang.Boolean](rows, ordinal, nulls, true)
        TColumn.boolVal(new TBoolColumn(values, nulls))

      case JDBCColumnType.TINYINT =>
        val values = getOrSetAsNull[java.lang.Byte](rows, ordinal, nulls, 0.toByte)
        TColumn.byteVal(new TByteColumn(values, nulls))

      case JDBCColumnType.SMALLINT =>
        val values = getOrSetAsNull[java.lang.Short](rows, ordinal, nulls, 0.toShort)
        TColumn.i16Val(new TI16Column(values, nulls))

      case JDBCColumnType.INTEGER =>
        val values = getOrSetAsNull[java.lang.Integer](rows, ordinal, nulls, 0)
        TColumn.i32Val(new TI32Column(values, nulls))

      case JDBCColumnType.BIGINT =>
        val values = getOrSetAsNull[java.lang.Long](rows, ordinal, nulls, 0L)
        TColumn.i64Val(new TI64Column(values, nulls))

      case JDBCColumnType.REAL =>
        val values = getOrSetAsNull[java.lang.Float](rows, ordinal, nulls, 0.toFloat)
          .asScala.map(n => java.lang.Double.valueOf(n.toString)).asJava
        TColumn.doubleVal(new TDoubleColumn(values, nulls))

      case JDBCColumnType.FLOAT =>
        val values = getOrSetAsNull[java.lang.Float](rows, ordinal, nulls, 0.toFloat)
          .asScala.map(n => java.lang.Double.valueOf(n.toString)).asJava
        TColumn.doubleVal(new TDoubleColumn(values, nulls))

      case JDBCColumnType.DOUBLE =>
        val values = getOrSetAsNull[java.lang.Double](rows, ordinal, nulls, 0.toDouble)
        TColumn.doubleVal(new TDoubleColumn(values, nulls))

      case JDBCColumnType.VARCHAR =>
        val values = getOrSetAsNull[String](rows, ordinal, nulls, "")
        TColumn.stringVal(new TStringColumn(values, nulls))

      case JDBCColumnType.LONGVARCHAR =>
        val values = getOrSetAsNull[String](rows, ordinal, nulls, "")
        TColumn.stringVal(new TStringColumn(values, nulls))

      case JDBCColumnType.LONGNVARCHAR =>
        val values = getOrSetAsNull[String](rows, ordinal, nulls, "")
        TColumn.stringVal(new TStringColumn(values, nulls))

      case JDBCColumnType.BINARY =>
        val values = getOrSetAsNull[Array[Byte]](rows, ordinal, nulls, Array())
          .asScala
          .map(ByteBuffer.wrap)
          .asJava
        TColumn.binaryVal(new TBinaryColumn(values, nulls))

      case _ =>
        val values = rows.zipWithIndex.map { case (row, i) =>
          nulls.set(i, row(ordinal) == null)
          if (row(ordinal) == null) {
            ""
          } else {
            jdbc2HiveString(row(ordinal), jdbcColumn)
          }
        }.asJava
        TColumn.stringVal(new TStringColumn(values, nulls))
    }
  }

  private def getOrSetAsNull[T](
      rows: Seq[Seq[Any]],
      ordinal: Int,
      nulls: java.util.BitSet,
      defaultVal: T): java.util.List[T] = {
    val size = rows.length
    val ret = new java.util.ArrayList[T](size)
    var idx = 0
    while (idx < size) {
      val row = rows(idx)
      val isNull = row(ordinal) == null
      if (isNull) {
        nulls.set(idx, true)
        ret.add(idx, defaultVal)
      } else {
        ret.add(idx, row(ordinal).asInstanceOf[T])
      }
      idx += 1
    }
    ret
  }

  private def jdbc2TColumnValue(
      ordinal: Int,
      row: List[Any],
      types: List[JDBCColumn]): TColumnValue = {

    types(ordinal).getJdbcColumnType match {
      case JDBCColumnType.BOOLEAN =>
        val boolValue = new TBoolValue
        if (row(ordinal) != null) boolValue.setValue(row(ordinal).asInstanceOf[Boolean])
        TColumnValue.boolVal(boolValue)

      case JDBCColumnType.TINYINT =>
        val byteValue = new TByteValue
        if (row(ordinal) != null) byteValue.setValue(row(ordinal).asInstanceOf[Byte])
        TColumnValue.byteVal(byteValue)

      case JDBCColumnType.SMALLINT =>
        val tI16Value = new TI16Value
        if (row(ordinal) != null) tI16Value.setValue(row(ordinal).asInstanceOf[Short])
        TColumnValue.i16Val(tI16Value)

      case JDBCColumnType.INTEGER =>
        val tI32Value = new TI32Value
        if (row(ordinal) != null) tI32Value.setValue(row(ordinal).asInstanceOf[Int])
        TColumnValue.i32Val(tI32Value)

      case JDBCColumnType.BIGINT =>
        val tI64Value = new TI64Value
        if (row(ordinal) != null) tI64Value.setValue(row(ordinal).asInstanceOf[Long])
        TColumnValue.i64Val(tI64Value)

      case JDBCColumnType.REAL =>
        val tDoubleValue = new TDoubleValue
        if (row(ordinal) != null) {
          val doubleValue = java.lang.Double.valueOf(row(ordinal).asInstanceOf[Float].toString)
          tDoubleValue.setValue(doubleValue)
        }
        TColumnValue.doubleVal(tDoubleValue)

      case JDBCColumnType.FLOAT =>
        val tDoubleValue = new TDoubleValue
        if (row(ordinal) != null) {
          val doubleValue = java.lang.Double.valueOf(row(ordinal).asInstanceOf[Float].toString)
          tDoubleValue.setValue(doubleValue)
        }
        TColumnValue.doubleVal(tDoubleValue)

      case JDBCColumnType.DOUBLE =>
        val tDoubleValue = new TDoubleValue
        if (row(ordinal) != null) tDoubleValue.setValue(row(ordinal).asInstanceOf[Double])
        TColumnValue.doubleVal(tDoubleValue)

      case JDBCColumnType.VARCHAR =>
        val tStringValue = new TStringValue
        if (row(ordinal) != null) tStringValue.setValue(row(ordinal).asInstanceOf[String])
        TColumnValue.stringVal(tStringValue)

      case _ =>
        val tStrValue = new TStringValue
        if (row(ordinal) != null) {
          tStrValue.setValue(
            jdbc2HiveString(row(ordinal), types(ordinal)))
        }
        TColumnValue.stringVal(tStrValue)
    }
  }

  /**
   * A simple impl of jdbc engine's to hive string
   */
  def jdbc2HiveString(data: Any, jdbcColumn: JDBCColumn): String = {
    (data, jdbcColumn.getJdbcColumnType) match {
      case (null, _) =>
        // Only match nulls in nested type values
        "null"

      case (bin: Array[Byte], JDBCColumnType.VARBINARY) =>
        new String(bin, StandardCharsets.UTF_8)

      case (bin: Array[Byte], JDBCColumnType.LONGVARBINARY) =>
        new String(bin, StandardCharsets.UTF_8)

      case (s: String, JDBCColumnType.VARCHAR) =>
        // Only match string in nested type values
        "\"" + s + "\""

      case (s: String, JDBCColumnType.LONGVARCHAR) =>
        // Only match string in nested type values
        "\"" + s + "\""

      case (s: String, JDBCColumnType.LONGNVARCHAR) =>
        // Only match string in nested type values
        "\"" + s + "\"";

      // TODO array 类型存在BUG, 需要在引擎段修复
      case (list: java.util.List[_], JDBCColumnType.ARRAY) =>
        checkArgument(
          // typ.getArgumentsAsTypeSignatures.asScala.nonEmpty,
          true,
          "Missing ARRAY argument type")
        // val listType = typ.getArgumentsAsTypeSignatures.get(0)
        // val listType = JDBCColumn.builder().jdbcColumnType(JDBCColumnType.VARCHAR).build()
        val listType = new JDBCColumn()
        listType.setJdbcColumnType(JDBCColumnType.VARCHAR)
        list.asScala
          .map(jdbc2HiveString(_, listType))
          .mkString("[", ",", "]")

      //      case (m: java.util.Map[_, _], JDBCColumnType.MAP) =>
      //        checkArgument(
      //          typ.getArgumentsAsTypeSignatures.size() == 2,
      //          "Mismatched number of MAP argument types")
      //        val keyType = typ.getArgumentsAsTypeSignatures.get(0)
      //        val valueType = typ.getArgumentsAsTypeSignatures.get(1)
      //        m.asScala.map { case (key, value) =>
      //          toHiveString(key, keyType) + ":" + toHiveString(value, valueType)
      //        }.toSeq.sorted.mkString("{", ",", "}")
      //
      //      case (row: Row, ROW) =>
      //        checkArgument(
      //          row.getFields.size() == typ.getArguments.size(),
      //          "Mismatched data values and ROW type")
      //        row.getFields.asScala.zipWithIndex.map { case (r, index) =>
      //          val namedRowType = typ.getArguments.get(index).getNamedTypeSignature
      //          if (namedRowType.getName.isPresent) {
      //            namedRowType.getName.get() + "=" +
      //              toHiveString(r.getValue, namedRowType.getTypeSignature)
      //          } else {
      //            toHiveString(r.getValue, namedRowType.getTypeSignature)
      //          }
      //        }.mkString("{", ",", "}")

      case (other, _) =>
        other.toString
    }
  }
}
