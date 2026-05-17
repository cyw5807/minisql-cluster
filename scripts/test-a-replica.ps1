Param()

$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot/.."

Write-Host "[A-测试] 副本管理模块"
mvn -q -pl minisql-common -am -Dtest=ReplicaManagerImplTest test
if ($LASTEXITCODE -ne 0) { throw "副本管理模块测试失败" }
Write-Host "[A-测试] 副本管理模块通过"
