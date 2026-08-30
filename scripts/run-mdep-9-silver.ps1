param(
  [Parameter(Mandatory = $true)][string]$BronzeRoot,
  [Parameter(Mandatory = $true)][string]$Warehouse,
  [string]$LogicalDate = "2025-02-01",
  [switch]$Inspect,
  [switch]$SkewExercise,
  [switch]$AdditiveSchemaEvolution
)

$args = @(
  "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
  "processing/spark/silver_batch.py", "--bronze-root", $BronzeRoot,
  "--warehouse", $Warehouse, "--logical-date", $LogicalDate
)
if ($Inspect) { $args += "--inspect" }
if ($SkewExercise) { $args += "--skew-exercise" }
if ($AdditiveSchemaEvolution) { $args += "--additive-schema-evolution" }
& spark-submit @args
