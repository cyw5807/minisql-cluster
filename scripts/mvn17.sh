#!/bin/bash
set -e

# 仅对当前命令临时切换到 JDK 17，不影响系统默认 Java。
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="/opt/homebrew/bin:$JAVA_HOME/bin:$PATH"

mvn "$@"
