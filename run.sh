#!/bin/bash
set -e
JAVA_VERSION=$(java -version 2>&1 | awk -F[\".] '/version/ {print $2}')
if [ "$JAVA_VERSION" -ge 17 ]; then
  echo "⚠️  WARNING: Java version $JAVA_VERSION detected. Spark may fail with Java 17+ due to UGI security context issues."
  echo "👉  Recommended: Set JAVA_HOME to a Java 11 installation:"
  echo "    export JAVA_HOME=\$(/usr/libexec/java_home -v11)"
  echo
fi

export PYSPARK_PYTHON="$(pwd)/.venv/bin/python"
export PYTHONPATH=src

spark-submit src/tensorstore_spark_loader/example_spark_job.py
