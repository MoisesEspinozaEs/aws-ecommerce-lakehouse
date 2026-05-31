terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}

locals {
  name = "${var.project}-${var.environment}"
  tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
    Owner       = "espinozamoises"
  }
}

# --------------------------------------------------------------------
# Lake storage: one bucket, three medallion prefixes, all Iceberg
# Account id suffix keeps the bucket name globally unique (S3 requires it)
# --------------------------------------------------------------------
resource "aws_s3_bucket" "lake" {
  bucket        = "${local.name}-lake-${data.aws_caller_identity.current.account_id}"
  force_destroy = true # demo project: allow terraform destroy to clean up
  tags          = local.tags
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "expire-bronze-raw"
    status = "Enabled"
    filter { prefix = "bronze/" }
    expiration { days = var.bronze_retention_days }
  }
}

# --------------------------------------------------------------------
# Streaming ingestion
# --------------------------------------------------------------------
resource "aws_kinesis_stream" "clickstream" {
  count = var.enable_streaming ? 1 : 0
  name  = "${local.name}-clickstream"
  stream_mode_details {
    stream_mode = "ON_DEMAND" # scales with traffic, no shard math
  }
  tags = local.tags
}

# --------------------------------------------------------------------
# Glue catalog databases per layer (Iceberg tables register here)
# --------------------------------------------------------------------
resource "aws_glue_catalog_database" "layers" {
  for_each = toset(["bronze", "silver", "gold"])
  name     = "${var.project}_${each.key}"
}

# --------------------------------------------------------------------
# Athena workgroup with results pointing at the lake
# --------------------------------------------------------------------
resource "aws_athena_workgroup" "wg" {
  name = local.name
  configuration {
    enforce_workgroup_configuration = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
    }
  }
  tags = local.tags
}

# --------------------------------------------------------------------
# IAM role used by Glue and the local batch path
# --------------------------------------------------------------------
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_lake_access" {
  name = "lake-access"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
    }]
  })
}

# Upload the Glue script to S3 so the job can find it
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/bronze_to_silver.py"
  source = "${path.module}/../glue/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../glue/bronze_to_silver.py")
}

# Glue job: bronze -> silver
resource "aws_glue_job" "bronze_to_silver" {
  name              = "${local.name}-bronze-to-silver"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${aws_s3_bucket.lake.bucket}/scripts/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"                 = "iceberg"
    "--lake_bucket"                      = aws_s3_bucket.lake.bucket
    "--enable-continuous-cloudwatch-log" = "true"
    # Iceberg extensions must be enabled at Spark session start, not from the
    # script. This is what makes MERGE INTO and the Glue Iceberg catalog work.
    "--conf" = join(" ", [
      "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
      "--conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lake.bucket}/",
      "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
      "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
      "--conf spark.sql.catalog.glue_catalog.glue.skip-archive=true",
    ])
  }

  tags = local.tags
}

# --------------------------------------------------------------------
# Firehose: Kinesis -> S3 bronze (raw clicks), no Flink needed for the demo
# --------------------------------------------------------------------
data "aws_iam_policy_document" "firehose_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  count              = var.enable_streaming ? 1 : 0
  name               = "${local.name}-firehose"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy" "firehose" {
  count = var.enable_streaming ? 1 : 0
  name  = "firehose-access"
  role  = aws_iam_role.firehose[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetBucketLocation", "s3:ListBucket"]
        Resource = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kinesis:DescribeStream", "kinesis:GetShardIterator", "kinesis:GetRecords", "kinesis:ListShards"]
        Resource = [aws_kinesis_stream.clickstream[0].arn]
      }
    ]
  })
}

resource "aws_kinesis_firehose_delivery_stream" "to_bronze" {
  count       = var.enable_streaming ? 1 : 0
  name        = "${local.name}-to-bronze"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.clickstream[0].arn
    role_arn           = aws_iam_role.firehose[0].arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose[0].arn
    bucket_arn          = aws_s3_bucket.lake.arn
    prefix              = "bronze/raw_clicks/"
    error_output_prefix = "bronze/errors/"
    buffering_size      = 1  # MB, small so the demo lands data fast
    buffering_interval  = 60 # seconds
    compression_format  = "GZIP"
  }

  tags = local.tags
}

# --------------------------------------------------------------------
# Budget alarm: emails you before anything gets expensive
# --------------------------------------------------------------------
resource "aws_budgets_budget" "monthly" {
  name         = "${local.name}-monthly"
  budget_type  = "COST"
  limit_amount = var.monthly_budget_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
}
