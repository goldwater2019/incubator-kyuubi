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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_JDBC_CONNECTION_URL

trait WithJDBCEngine extends KyuubiFunSuite with WithJDBCContainerServer {
  protected var engine: JDBCSqlEngine = _
  protected var connectionUrl: String = "http://127.0.0.1:8080/"

  override val kyuubiConf: KyuubiConf = JDBCSqlEngine.kyuubiConf

  protected var withKyuubiConf: Map[String, String] = _

  override def beforeAll(): Unit = {
    startJDBCEngine()
    super.beforeAll()
  }

  def startJDBCEngine(): Unit = {
    kyuubiConf.set(ENGINE_JDBC_CONNECTION_URL, connectionUrl)
    if (withKyuubiConf != null && withKyuubiConf.size > 0) {
      withKyuubiConf.foreach {
        case (k, v) =>
          System.setProperty(k, v)
          kyuubiConf.set(k, v)
      }
    }

    JDBCSqlEngine.startEngine()
    engine = JDBCSqlEngine.currentEngine.get
  }

  override def afterAll(): Unit = {
    stopJDBCEngine()
    super.afterAll()
  }

  def stopJDBCEngine(): Unit = {
    if (engine != null) {
      engine.stop()
    }
    engine = null
  }
}
