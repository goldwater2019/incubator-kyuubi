#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# 确认JAVA_HOME是否成功配置
if [[ -z ${JAVA_HOME} ]]; then
  echo "[ERROR] JAVA_HOME IS NOT SET! CANNOT PROCEED."
  exit 1
fi

# 获得RUNNER
RUNNER="${JAVA_HOME}/bin/java"

# 如果是默认集成的三方插件, 则使用默认的配置, 调用的是编译出来的三方插件
if [[ "$TRINO_ENGINE_HOME" == "$KYUUBI_HOME/externals/engines/trino" ]]; then
  TRINO_CLIENT_JAR="$TRINO_ENGINE_JAR"
  TRINO_CLIENT_JARS_DIR="$TRINO_ENGINE_HOME/jars"
else
  echo "\nTRINO_ENGINE_HOME $TRINO_ENGINE_HOME doesn't match production directory, assuming in development environment..."
  TRINO_CLIENT_JAR=$(find $TRINO_ENGINE_HOME/target -regex '.*/kyuubi-trino-engine_.*.jar$' | grep -v '\-sources.jar$' | grep -v '\-javadoc.jar$' | grep -v '\-tests.jar$')
  TRINO_CLIENT_JARS_DIR=$(find $TRINO_ENGINE_HOME/target -regex '.*/jars')
fi

TRINO_CLIENT_CLASSPATH="$TRINO_CLIENT_JARS_DIR/*"
FULL_CLASSPATH="$TRINO_CLIENT_CLASSPATH:$TRINO_CLIENT_JAR"

if [ -n "$TRINO_CLIENT_JAR" ]; then
  # 脚本透传进来的 TRINO_ENGINE_DYNAMIC_ARGS 和 FULL_CLASSPATH
  exec $RUNNER ${TRINO_ENGINE_DYNAMIC_ARGS} -cp ${FULL_CLASSPATH} org.apache.kyuubi.engine.trino.TrinoSqlEngine "$@"
else
  (>&2 echo "[ERROR] TRINO Engine JAR file 'kyuubi-trino-engine*.jar' should be located in $TRINO_ENGINE_HOME/jars.")
  exit 1
fi

# TODO 使用脚本的方式引导启动 jdbc-engine-sql_2.12.jar