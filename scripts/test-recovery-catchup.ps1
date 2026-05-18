Param()

$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot/.."

Write-Host "[Recovery] 1/2 replica manager gap + rejoin catch-up"
mvn -q -pl minisql-common -am -Dtest=ReplicaManagerImplTest test
if ($LASTEXITCODE -ne 0) { throw "replica manager recovery tests failed" }

Write-Host "[Recovery] 2/2 worker side gap detection and replay"
mvn -q -pl minisql-worker -am -Dtest=ReplicaDataSyncServiceImplTest "-Dsurefire.failIfNoSpecifiedTests=false" test
if ($LASTEXITCODE -ne 0) { throw "worker recovery tests failed" }

Write-Host "[Recovery] all catch-up checks passed"
