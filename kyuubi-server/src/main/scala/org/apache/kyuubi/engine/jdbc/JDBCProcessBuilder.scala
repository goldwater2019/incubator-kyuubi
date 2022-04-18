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

import java.net.URI
import java.nio.file.Files
import java.nio.file.Paths

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiSQLException, Logging, SCALA_COMPILE_VERSION}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_CONNECTION_URL, ENGINE_JDBC_MAIN_RESOURCE, ENGINE_JDBC_QUERY_ROUTE}
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.engine.jdbc.JDBCProcessBuilder.{JDBC_ENGINE_BINARY_FILE, USER}
import org.apache.kyuubi.operation.log.OperationLog


class JDBCProcessBuilder (
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val extraEngineLog: Option[OperationLog] = None) extends ProcBuilder with Logging {

  private[jdbc] lazy val jdbcConf: Map[String, String] = {
    assert(
      conf.get(ENGINE_JDBC_CONNECTION_URL).isDefined,
      throw KyuubiSQLException(
        s"JDBC engine server url can not bu null! please set ${ENGINE_JDBC_CONNECTION_URL.key}"
      )
    )
    assert(
      conf.get(ENGINE_JDBC_QUERY_ROUTE).isDefined,
      throw KyuubiSQLException(
        s"JDBC engine server query route can not bu null! please set ${ENGINE_JDBC_QUERY_ROUTE.key}"
      )
    )
    conf.getAll.filter {
      case (k, v) => !k.startsWith("hadoop.") && !k.startsWith("spark.")
    } + (USER -> proxyUser)
  }

  override protected val executable: String = {
    val jdbcHomeOpt = env.get("JDBC_ENGINE_HOME").orElse {
      val cwd = getClass.getProtectionDomain.getCodeSource.getLocation.getPath
        .split("kyuubi-server")
      assert(cwd.length > 1)
      Option(
        Paths.get(cwd.head)
          .resolve("external")
          .resolve(module)
          .toFile
      ).map(
        _.getAbsolutePath
      )
    }
    jdbcHomeOpt
    jdbcHomeOpt.map {
      dir => Paths.get(dir, "bin", JDBC_ENGINE_BINARY_FILE).toAbsolutePath.toFile.getCanonicalPath
    }.getOrElse {
      throw KyuubiSQLException("JDBC_ENGINE_HOME is not set! " +
        "For more detail information on installing and configuring JDBC engine, " +
        "please visit github")
    }
  };

  // 没有 jdbc-engine-sql_2.12-1.5.1.jar的话不能正常运行
  override protected def mainResource: Option[String] = {
    val jarName = s"${module}_${SCALA_COMPILE_VERSION}-${KYUUBI_VERSION}.jar"
    // 1. get the main resource jar for user specified config
    conf.get(ENGINE_JDBC_MAIN_RESOURCE).filter{ userSpecified =>
      val uri = new URI(userSpecified)
      val schema = if (uri.getScheme != null) uri.getScheme else "file"
      schema match {
        case "file" => Files.exists(Paths.get(userSpecified))
        case _ => true
      }
    }.orElse {
      // 2. get the main resource jar from system build default
      env.get(KyuubiConf.KYUUBI_HOME)
        .map{ Paths.get(_, "externals", "engines", "jdbc", "jars", jarName)}
        .filter{ Files.exists(_)}.map(_.toAbsolutePath.toFile.getCanonicalPath)
    }.orElse{
      // 3. get the main resource from dev environment
      Option(Paths.get("externals", module, "target", jarName))
        .filter{Files.exists(_)}.orElse {
        Some(Paths.get("..", "externals", module, "target", jarName))
      }.map(_.toAbsolutePath.toFile.getCanonicalPath)
    }
  };

  // TODO JDBC 侧自定义配置
  override protected def module: String = "kyuubi-jdbc-engine";
  // TODO engine侧的实现
  override protected def mainClass: String = "org.apahe.kyuubi.engine.jdbc.JDBCSqlEngine";
  // TODO 环境参数配置
  override protected def childProcEnv: Map[String, String] = conf.getEnvs +
    ("JDBC_ENGINE_JAR" -> mainResource.get) +
    ("JDBC_ENGINE_DYNAMIC_ARGS" ->
      jdbcConf.map {
        case(k, v) => s"-D$k=$v"
      }.mkString(" "))

  override protected def commands: Array[String] = Array(executable);

  /**
   * 将相应的启动参数格式化
   * @return
   */
  override def toString: String = commands.map {
    case arg if arg.startsWith("--") => s"\\\n\t$arg"
    case arg => arg
  }.mkString(" ")

}


object JDBCProcessBuilder {
  final private val USER = "kyuubi.jdbc.user"
  final private val JDBC_ENGINE_BINARY_FILE = "jdbc-engine.sh"
}