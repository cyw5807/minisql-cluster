Param()

$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot/.."

Write-Host "[A-测试] 负载均衡模块"
mvn -q -pl minisql-common -am -Dtest=LoadBalancerImplTest test
if ($LASTEXITCODE -ne 0) { throw "负载均衡模块测试失败" }
Write-Host "[A-测试] 负载均衡模块通过"
