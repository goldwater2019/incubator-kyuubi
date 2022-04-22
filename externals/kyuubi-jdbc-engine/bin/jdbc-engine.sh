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
if [[ -z ${JAVA_HOME} ]]; then
  echo "[ERROR] JAVA_HOME IS NOT SET! CANNOT PROCEED."
  exit 1
fi

RUNNER="${JAVA_HOME}/bin/java"

if [[ "$JDBC_ENGINE_HOME" == "$KYUUBI_HOME/externals/engines/jdbc" ]]; then
  JDBC_CLIENT_JAR="$JDBC_ENGINE_JAR"
  JDBC_CLIENT_JARS_DIR="$JDBC_ENGINE_HOME/jars"
else
  echo "\nJDBC_ENGINE_HOME $JDBC_ENGINE_HOME doesn't match production directory, assuming in development environment..."
  JDBC_CLIENT_JAR=$(find $JDBC_ENGINE_HOME/target -regex '.*/kyuubi-jdbc-engine_.*.jar$' | grep -v '\-sources.jar$' | grep -v '\-javadoc.jar$' | grep -v '\-tests.jar$')
  JDBC_CLIENT_JARS_DIR=$(find $JDBC_ENGINE_HOME/target -regex '.*/jars')
fi

JDBC_CLIENT_CLASSPATH=$(find $JDBC_CLIENT_JARS_DIR -regex ".*jar" | tr '\n' ':')
FULL_CLASSPATH="$JDBC_CLIENT_CLASSPATH$JDBC_CLIENT_JAR"

if [ -n "$JDBC_CLIENT_JAR" ]; then
  exec $RUNNER ${JDBC_ENGINE_DYNAMIC_ARGS} -cp ${FULL_CLASSPATH} org.apache.kyuubi.engine.jdbc.JDBCSqlEngine "$@"
  echo $RUNNER ${JDBC_ENGINE_DYNAMIC_ARGS} -cp ${FULL_CLASSPATH} org.apache.kyuubi.engine.jdbc.JDBCSqlEngine "$@" > /Users/zhangxinsen/workspace/incubator-kyuubi/conf/scripts.log
else
  (>&2 echo $RUNNER ${JDBC_ENGINE_DYNAMIC_ARGS} -cp ${FULL_CLASSPATH} org.apache.kyuubi.engine.jdbc.JDBCSqlEngine "$@")
  (>&2 echo "[ERROR] JDBC Engine JAR file 'kyuubi-jdbc-engine*.jar' should be located in $JDBC_ENGINE_HOME/jars.")
  exit 1
fi
