#!/bin/bash
set -e

PORT=${1:-9012}
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="/opt/homebrew/bin:$JAVA_HOME/bin:$PATH"

# 确保依赖模块已安装到本地仓库，避免按模块运行时缺少 SNAPSHOT 依赖。
./scripts/prepare-demo.sh

# 直接复用项目中的 WorkerStarter，并通过启动参数指定端口。
mvn -q -f minisql-worker/pom.xml -DskipTests exec:java \
  -Dexec.mainClass=com.zju.minisql.worker.WorkerStarter \
  -Dexec.args="$PORT"
