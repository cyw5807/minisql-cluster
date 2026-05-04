#!/bin/bash
set -e

# 仅为当前脚本注入 JDK 17，避免修改系统级 JAVA_HOME。
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export ZOOCFGDIR=/opt/homebrew/etc/zookeeper
export ZOO_LOG_DIR=/opt/homebrew/var/log/zookeeper
export ZOOPIDFILE=/opt/homebrew/var/run/zookeeper/zookeeper_server.pid

/opt/homebrew/Cellar/zookeeper/3.9.5/libexec/bin/zkServer.sh --config /opt/homebrew/etc/zookeeper start
