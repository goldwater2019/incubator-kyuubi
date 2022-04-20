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

import io.trino.client.ClientStandardTypes._
import io.trino.client.{ClientTypeSignature, Column}
import org.apache.hive.service.rpc.thrift._
import org.apache.kyuubi.engine.jdbc.enumeration.JDBCColumnType

import java.util.{Collections, Locale}
import scala.collection.JavaConverters._

object JDBCSchemaHelper {

  private lazy val STRING_TYPES = Set(
    HYPER_LOG_LOG,
    QDIGEST,
    P4_HYPER_LOG_LOG,
    TIMESTAMP_WITH_TIME_ZONE,
    TIME,
    TIME_WITH_TIME_ZONE,
    JSON,
    IPADDRESS,
    UUID,
    GEOMETRY,
    SPHERICAL_GEOGRAPHY,
    BING_TILE)

  /**
   * BIT => TINYINT
   * NUMERICAL => DECIMAL
   * VARCHAR => STRING
   * LONGVATCHAR => STRING
   * TIME => STRING
   * VARBINARY => BINARY
   * LONGVARBINARY => BINARY
   * REAL => STRING
   * @param jdbcColumnType
   * @return
   */
  def jdbc2TTypeId(jdbcColumnType: JDBCColumnType) : TTypeId = jdbcColumnType match {
    case JDBCColumnType.BIT => TTypeId.TINYINT_TYPE
    case JDBCColumnType.TINYINT => TTypeId.TINYINT_TYPE
    case JDBCColumnType.SMALLINT => TTypeId.SMALLINT_TYPE
    case JDBCColumnType.INTEGER => TTypeId.INT_TYPE
    case JDBCColumnType.BIGINT => TTypeId.BIGINT_TYPE
    case JDBCColumnType.FLOAT => TTypeId.FLOAT_TYPE
    case JDBCColumnType.REAL => TTypeId.STRING_TYPE
    case JDBCColumnType.DOUBLE => TTypeId.DOUBLE_TYPE
    case JDBCColumnType.NUMERIC => TTypeId.DECIMAL_TYPE
    case JDBCColumnType.DECIMAL => TTypeId.DECIMAL_TYPE
    case JDBCColumnType.CHAR => TTypeId.CHAR_TYPE
    case JDBCColumnType.VARCHAR => TTypeId.STRING_TYPE
    case JDBCColumnType.LONGVARCHAR => TTypeId.STRING_TYPE
    case JDBCColumnType.DATE => TTypeId.DATE_TYPE
    case JDBCColumnType.TIME => TTypeId.STRING_TYPE
    case JDBCColumnType.TIMESTAMP => TTypeId.TIMESTAMP_TYPE
    case JDBCColumnType.BINARY => TTypeId.BINARY_TYPE
    case JDBCColumnType.VARBINARY => TTypeId.BINARY_TYPE
    case JDBCColumnType.LONGVARBINARY => TTypeId.BINARY_TYPE
    case JDBCColumnType.NULL => TTypeId.NULL_TYPE
    case JDBCColumnType.STRUCT => TTypeId.STRUCT_TYPE
    case JDBCColumnType.ARRAY => TTypeId.ARRAY_TYPE
    case JDBCColumnType.BOOLEAN => TTypeId.BOOLEAN_TYPE
    case JDBCColumnType.LONGNVARCHAR => TTypeId.STRING_TYPE
    case JDBCColumnType.OTHER =>
      throw new IllegalArgumentException("Unrecognized jdbc type, other type")
  }

  /**
   *  DECIMAL, NUMERICAL, 默认精度decimal(32,3)
   * @param jdbcColumnType
   * @return
   */
  def jdbcToTTypeQualifiers(jdbcColumnType: JDBCColumnType): TTypeQualifiers = {
    val ret = new TTypeQualifiers()
    val qualifiers = jdbcColumnType match {
      case JDBCColumnType.DECIMAL =>
        Map(
          TCLIServiceConstants.PRECISION ->
            TTypeQualifierValue.i32Value(32),
          TCLIServiceConstants.SCALE ->
            TTypeQualifierValue.i32Value(3)
        )
          .asJava
      case JDBCColumnType.NUMERIC =>
        Map(
          TCLIServiceConstants.PRECISION ->
            TTypeQualifierValue.i32Value(32),
          TCLIServiceConstants.SCALE ->
            TTypeQualifierValue.i32Value(3)
        )
          .asJava
      case _ => Collections.emptyMap[String, TTypeQualifierValue]()
    }
    ret.setQualifiers(qualifiers)
    ret
  }

  def toTTypeQualifiers(typ: ClientTypeSignature): TTypeQualifiers = {
    val ret = new TTypeQualifiers()
    val qualifiers = typ.getRawType match {
      case DECIMAL =>
        Map(
          TCLIServiceConstants.PRECISION ->
            TTypeQualifierValue.i32Value(typ.getArguments.get(0).getValue.asInstanceOf[Long].toInt),
          TCLIServiceConstants.SCALE ->
            TTypeQualifierValue.i32Value(typ.getArguments.get(1).getValue.asInstanceOf[Long].toInt))
          .asJava
      case _ => Collections.emptyMap[String, TTypeQualifierValue]()
    }
    ret.setQualifiers(qualifiers)
    ret
  }

  def toTTypeDesc(typ: ClientTypeSignature): TTypeDesc = {
    val typeEntry = new TPrimitiveTypeEntry(toTTypeId(typ))
    typeEntry.setTypeQualifiers(toTTypeQualifiers(typ))
    val tTypeDesc = new TTypeDesc()
    tTypeDesc.addToTypes(TTypeEntry.primitiveEntry(typeEntry))
    tTypeDesc
  }

  // TODO jdbcEngine侧, column信息
  def toTColumnDesc(column: Column, pos: Int): TColumnDesc = {
    val tColumnDesc = new TColumnDesc()
    tColumnDesc.setColumnName(column.getName)
    tColumnDesc.setTypeDesc(toTTypeDesc(column.getTypeSignature))
    tColumnDesc.setPosition(pos)
    tColumnDesc
  }

  def toTTableSchema(schema: Seq[Column]): TTableSchema = {
    val tTableSchema = new TTableSchema()
    schema.zipWithIndex.foreach { case (f, i) =>
      tTableSchema.addToColumns(toTColumnDesc(f, i))
    }
    tTableSchema
  }
}
