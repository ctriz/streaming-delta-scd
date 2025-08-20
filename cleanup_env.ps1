# cleanup_env.ps1

<# param (
    [switch]$DeleteKafkaTopics  # optional flag
)

Write-Host "Cleaning up old Spark Delta and checkpoint folders..."

$paths = @(
    "C:\UpScale\AgenticAI\stock-analyzer\stream-delta-scd\deltaquery_scd2",
    "C:\UpScale\AgenticAI\stock-analyzerstream-delta-scd\deltaquery"
)

foreach ($path in $paths) {
    if (Test-Path $path) {
        Remove-Item -Recurse -Force $path
        Write-Host "Deleted: $path"
    }
}

if ($DeleteKafkaTopics) {
    Write-Host "Deleting old Kafka topics..."
    & "C:\kafka\bin\windows\kafka-topics.bat" --delete --topic test_topic --bootstrap-server localhost:9092
    & "C:\kafka\bin\windows\kafka-topics.bat" --delete --topic test_topic_scd2 --bootstrap-server localhost:9092
}

Write-Host "Cleanup completed!"
# Usage: .\cleanup_env.ps1 -DeleteKafkaTopics
# To run without deleting Kafka topics: .\cleanup_env.ps1   
# To run with deleting Kafka topics: .\cleanup_env.ps1 -DeleteKafkaTopics
# Ensure you run this script with appropriate permissions to delete files and Kafka topics. #>

Remove-Item "C:\UpScale\AgenticAI\stock-analyzer\stream-delta-scd\deltaquery\employees_scd2" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item "C:\UpScale\AgenticAI\stock-analyzer\stream-delta-scd\deltaquery\_checkpoints\employees_scd2" -Recurse -Force -ErrorAction SilentlyContinue
