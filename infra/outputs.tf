output "lake_bucket" {
  description = "S3 bucket backing the lakehouse"
  value       = aws_s3_bucket.lake.bucket
}

output "clickstream_name" {
  description = "Kinesis stream to send click events to (null when streaming is disabled)"
  value       = var.enable_streaming ? aws_kinesis_stream.clickstream[0].name : null
}

output "athena_workgroup" {
  description = "Athena workgroup for querying gold"
  value       = aws_athena_workgroup.wg.name
}

output "glue_databases" {
  description = "Glue catalog databases per medallion layer"
  value       = [for db in aws_glue_catalog_database.layers : db.name]
}
