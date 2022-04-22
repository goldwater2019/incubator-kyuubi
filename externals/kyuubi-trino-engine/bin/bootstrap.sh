#!/usr/bin/env bash
TRINO_ENGINE_HOME="/Users/zhangxinsen/workspace/incubator-kyuubi/externals/kyuubi-trino-engine"
TMP=$(find $TRINO_ENGINE_HOME/target -regex ".*/jars/.*\.jar" | tr '\n' ':')
TRINO_CLIENT_CLASSPATH=${${TMP}/jar\n/jar:}
TRINO_CLIENT_JAR=$(find $TRINO_ENGINE_HOME/target -regex '.*/kyuubi-trino-engine_.*.jar$' | grep -v '\-sources.jar$' | grep -v '\-javadoc.jar$' | grep -v '\-tests.jar$')
FULL_CLASSPATH="$TRINO_CLIENT_CLASSPATH$TRINO_CLIENT_JAR"
#java -cp  $FULL_CLASSPATH org.apache.kyuubi.engine.trino.TrinoSqlEngine

TRINO_CLIENT_JARS_DIR=/Users/zhangxinsen/workspace/incubator-kyuubi/externals/kyuubi-trino-engine/target/scala-2.12/jars
$TRINO_CLIENT_CLASSPATH=$(find $TRINO_CLIENT_JARS_DIR -regex ".*jar" | tr '\n' ':')
echo $TRINO_CLIENT_CLASSPATH