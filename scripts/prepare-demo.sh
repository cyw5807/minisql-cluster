#!/bin/bash
set -e

# 将多模块构件安装到本地 Maven 仓库，便于后续按模块直接运行主类。
./scripts/mvn17.sh -q -DskipTests install
