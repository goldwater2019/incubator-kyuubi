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

package org.apache.kyuubi.engine.trino

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import com.alibaba.fastjson.{JSON, TypeReference}
import io.trino.client.ClientSession
import okhttp3.{MediaType, OkHttpClient, Request, RequestBody}

case class JDBCContext(
    httpClient: OkHttpClient,
    clientSession: AtomicReference[ClientSession])

object JDBCContext {
  def apply(httpClient: OkHttpClient, clientSession: ClientSession): JDBCContext =
    JDBCContext(httpClient, new AtomicReference(clientSession))

  val JDBCUrl: String = "http://localhost:8080/driver/query"

  def main(args: Array[String]): Unit = {
    val httpClient = new OkHttpClient.Builder().build();
    val jdbcQueryRed = new JDBCQueryReq("select * from aliyun.engine.t_click_logs limit 100,2")
    val requestJSON = "{\"querySql\": \"" + jdbcQueryRed.querySql + "\"}"
    val requestBody = RequestBody.create(MediaType.parse("application/json"), requestJSON)
    val request = new Request.Builder().url(JDBCUrl).post(requestBody).build()
    val response = httpClient.newCall(request).execute()
    val bodyString = response.body().string();
    val jsonResult = JSON.parseObject(bodyString, new TypeReference[JSONResult[JDBCResultRef]]() {})
    // scalastyle:off println
    val jdbcResultRef = jsonResult.data
    val jdbcOperationRef = jdbcResultRef.jdbcOperationRef
    val jdbcResultSet = jdbcResultRef.jdbcResultSet
    val resultRowList: Array[ColumnList] = jdbcResultSet.resultRowList
    for (elem <- resultRowList) {
      val columnList = elem.columnList  // row data
      for (elem <- columnList) {
        println(elem)
      }
    }
    // scalastyle:on println
  }
}

object QueryStatus extends Enumeration {
  val OK = Value(0)
  val FAILED = Value(1)
}

case class JDBCOperationRef(
    val startTime: Long,
    val endTime: Long,
    val catalogName: String,
    val operationRefId: UUID,
    val sqlStatement: String,
    val queryStatus: String,
    val message: String)

case class Column(
    val columnName: String,
    val columnType: String,
    val columnClassName: String,
    val columnValue: String)

case class ColumnList(val columnList: Array[Column])
case class JDBCResultSet(val resultRowList: Array[ColumnList]);

case class JDBCResultRef(val jdbcOperationRef: JDBCOperationRef, val jdbcResultSet: JDBCResultSet)
case class JSONResult[T](val data: T, val code: Int, val msg: String)
case class JDBCQueryReq(var querySql: String)
