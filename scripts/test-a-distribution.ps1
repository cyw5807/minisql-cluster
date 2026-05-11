Param()

$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot/.."

Write-Host "[A-测试] 数据分布模块"
mvn -q -pl minisql-common -am -Dtest=DistributionManagerImplTest test
if ($LASTEXITCODE -ne 0) { throw "数据分布模块测试失败" }
Write-Host "[A-测试] 数据分布模块通过"
