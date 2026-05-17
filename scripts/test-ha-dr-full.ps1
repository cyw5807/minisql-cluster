Param()

$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot/.."

Write-Host "[HA/DR] 1/5 distribution"
mvn -q -pl minisql-common -am -Dtest=DistributionManagerImplTest test
if ($LASTEXITCODE -ne 0) { throw "distribution regression failed" }

Write-Host "[HA/DR] 2/5 replica manager (failover retry)"
mvn -q -pl minisql-common -am -Dtest=ReplicaManagerImplTest test
if ($LASTEXITCODE -ne 0) { throw "replica manager regression failed" }

Write-Host "[HA/DR] 3/5 replica sync rpc"
mvn -q -pl minisql-client -am -Dtest=ReplicaSyncRpcTransportTest "-Dsurefire.failIfNoSpecifiedTests=false" test
if ($LASTEXITCODE -ne 0) { throw "replica sync rpc regression failed" }

Write-Host "[HA/DR] 4/5 load balance"
mvn -q -pl minisql-common -am -Dtest=LoadBalancerImplTest test
if ($LASTEXITCODE -ne 0) { throw "load balance regression failed" }

Write-Host "[HA/DR] 5/5 local storage"
mvn -q -pl minisql-worker -am -Dtest=LocalStorageEngineImplTest "-Dsurefire.failIfNoSpecifiedTests=false" test
if ($LASTEXITCODE -ne 0) { throw "local storage regression failed" }

Write-Host "[HA/DR] all checks passed (local mode, no docker)"
