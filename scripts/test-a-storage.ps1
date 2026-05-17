Param()

$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot/.."

Write-Host "[A-测试] 本地存储引擎模块"
mvn -q -pl minisql-worker -am -Dtest=LocalStorageEngineImplTest "-Dsurefire.failIfNoSpecifiedTests=false" test
if ($LASTEXITCODE -ne 0) { throw "本地存储引擎模块测试失败" }
Write-Host "[A-测试] 本地存储引擎模块通过"
