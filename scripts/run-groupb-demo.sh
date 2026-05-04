#!/bin/bash
set -e

export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="/opt/homebrew/bin:$JAVA_HOME/bin:$PATH"

# 确保依赖模块已安装到本地仓库，避免按模块运行时缺少 SNAPSHOT 依赖。
./scripts/prepare-demo.sh

if [ "$#" -eq 0 ]; then
  mvn -q -f minisql-master/pom.xml -DskipTests exec:java \
    -Dexec.mainClass=com.zju.minisql.master.demo.GroupBQueryDemo
else
  mvn -q -f minisql-master/pom.xml -DskipTests exec:java \
    -Dexec.mainClass=com.zju.minisql.master.demo.GroupBQueryDemo \
    -Dexec.args="127.0.0.1:2181 2 $*"
fi
