#!/bin/bash
set -e

# 使用与启动脚本一致的本地变量，确保停止的是同一个 ZK 实例。
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export ZOOCFGDIR=/opt/homebrew/etc/zookeeper
export ZOO_LOG_DIR=/opt/homebrew/var/log/zookeeper
export ZOOPIDFILE=/opt/homebrew/var/run/zookeeper/zookeeper_server.pid

/opt/homebrew/Cellar/zookeeper/3.9.5/libexec/bin/zkServer.sh --config /opt/homebrew/etc/zookeeper stop
