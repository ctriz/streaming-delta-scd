# run_spark.ps1
param (
    [Parameter(Mandatory=$true)]
    [string]$Script
)

# Path to your Spark home
$SPARK_HOME = "C:\spark\spark-3.4.1-bin-hadoop3"

# Path to your venv python
$VENV_PYTHON = "C:\UpScale\AgenticAI\stock-analyzer\stream-delta-scd\spark-delta\Scripts\python.exe"

# Full path to the script
$SCRIPT_PATH = Resolve-Path $Script

Write-Host "Running Spark job: $SCRIPT_PATH"
Write-Host "Using Python: $VENV_PYTHON"

& "$SPARK_HOME\bin\spark-submit.cmd" `
    --conf "spark.pyspark.python=$VENV_PYTHON" `
    --conf "spark.pyspark.driver.python=$VENV_PYTHON" `
    "$SCRIPT_PATH"

    