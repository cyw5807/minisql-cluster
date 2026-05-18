Param()

$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot/.."

Write-Host "[修复验证] 1/3 运行副本管理模块测试"
mvn -q -pl minisql-common -am -Dtest=ReplicaManagerImplTest test
if ($LASTEXITCODE -ne 0) { throw "副本管理模块测试失败" }

Write-Host "[修复验证] 2/3 运行副本 RPC 同步测试"
mvn -q -pl minisql-client -am -Dtest=ReplicaSyncRpcTransportTest "-Dsurefire.failIfNoSpecifiedTests=false" test
if ($LASTEXITCODE -ne 0) { throw "副本 RPC 同步测试失败" }

Write-Host "[修复验证] 3/3 运行副本断线补偿测试"
mvn -q -pl minisql-worker -am -Dtest=ReplicaDataSyncServiceImplTest "-Dsurefire.failIfNoSpecifiedTests=false" test
if ($LASTEXITCODE -ne 0) { throw "副本断线补偿测试失败" }

Write-Host "[修复验证] 全部通过"
