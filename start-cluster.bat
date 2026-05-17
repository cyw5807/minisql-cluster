@echo off
chcp 65001 > nul
echo ====================================================
echo       MiniSQL 分布式集群一键初始化脚本 (Windows CMD)   
echo ====================================================

:: 1. 彻底清理残局（规避端口冲突、内存残留及幽灵数据）
echo --^> [1/4] 正在物理清理旧容器及本地虚拟数据卷...
call docker-compose down -v

:: 2. 全局编译并安装 Maven 依赖
echo --^> [2/4] 正在对全量工程执行编译与本地基线安装 (mvn clean install)...
call mvn clean install -DskipTests
if %errorlevel% neq 0 (
    echo ❌ 错误: Maven 编译打包失败，请检查控制台报错源码！
    pause
    exit /b %errorlevel%
)

:: 3. 强制无缓存重构 Docker 镜像（规避多节点因版本不一致引发反序列化异常）
echo --^> [3/4] 正在强制无缓存重新构建分布式计算节点镜像...
call docker-compose build --no-cache

:: 4. 异步拉起分布式拓扑集群
echo --^> [4/4] 正在全量启动分布式底层算力集群 (ZK, Master*2, Worker*3)...
call docker-compose up -d

echo ====================================================
echo MiniSQL 分布式集群一键初始化成功！当前运行拓扑状态如下：
call docker-compose ps
echo ====================================================
echo 请直接复制运行以下终端命令拉起交互式客户端 Shell 面板：
echo mvn -q -f minisql-client/pom.xml "-DskipTests" exec:java "-Dexec.mainClass=com.zju.minisql.client.MiniSQLShell"
echo ====================================================
pause