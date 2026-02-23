terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
}

###########################################################################
####################### S3 bucket for file storage ########################
###########################################################################
resource "aws_s3_bucket" "state_bucket" {
  bucket = "${local.account_id}-cta-analytics-project"

  tags = {
    Project     = "cta-analytics-app"
    Environment = "PROD"
  }
}

resource "aws_s3_bucket_versioning" "state_bucket_versioning" {
  bucket = aws_s3_bucket.state_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "s3_public_access_block" {
  bucket = aws_s3_bucket.state_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Set versioning to remove noncurrent files in this bucket
resource "aws_s3_bucket_lifecycle_configuration" "s3_expire_old_versions" {
  depends_on = [aws_s3_bucket_versioning.state_bucket_versioning]

  bucket = aws_s3_bucket.state_bucket.id

  rule {
    id     = "expire_noncurrent_versions"
    status = "Enabled"

    # Terraform requires specifying a prefix so leaving empty string
    filter {
      prefix = ""
    }

    noncurrent_version_expiration {
      noncurrent_days = 14
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    # Terraform requires specifying a prefix so leaving empty string
    filter {
      prefix = ""
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

###########################################################################
#################### Generic Lambda Assume Role Policy ####################
###########################################################################
data "aws_iam_policy_document" "lambda_assume_role_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole"
    ]
  }
}

###########################################################################
######################### GTFS Data Fetch Lambda ##########################
###########################################################################
resource "aws_iam_role" "gtfs_data_fetch_role" {
  name               = "gtfs-data-fetch-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role_policy.json
  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

data "aws_iam_policy_document" "scheduler_assume_role_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole"
    ]
  }
}

data "aws_iam_policy_document" "gtfs_data_fetch_lambda_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "ssm:GetParameter",
      "ssm:PutParameter"
    ]

    resources = [
      "arn:aws:ssm:us-east-1:${local.account_id}:parameter/gtfs_last_modified_time"
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:PutObject"
    ]

    resources = [
      "arn:aws:s3:::${local.account_id}-cta-analytics-project/*"
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]

    resources = [
      "arn:aws:logs:us-east-1:${local.account_id}:log-group:/aws/lambda/gtfs-data-fetch:*"
    ]
  }
}

resource "aws_iam_role_policy" "gtfs_data_fetch_lambda_policy" {
  name   = "gtfs-data-fetch-lambda-policy"
  role   = aws_iam_role.gtfs_data_fetch_role.id
  policy = data.aws_iam_policy_document.gtfs_data_fetch_lambda_policy_document.json
}

resource "aws_lambda_function" "gtfs_data_fetch_lambda" {
  function_name                  = "gtfs-data-fetch"
  description                    = "Lambda function to fetch GTFS data"
  role                           = aws_iam_role.gtfs_data_fetch_role.arn
  handler                        = "lambdas.gtfs_data_fetch.main.handler"
  runtime                        = "python3.13"
  filename                       = "../../lambdas/gtfs_data_fetch/deployment_package.zip"
  source_code_hash               = filebase64sha256("../../lambdas/gtfs_data_fetch/deployment_package.zip")
  timeout                        = 60
  memory_size                    = 1024
  environment {
    variables = {
      ACCOUNT_NUMBER = local.account_id
    }
  }
  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

###########################################################################
#################### GTFS Data Fetch Lambda Scheduler #####################
###########################################################################

resource "aws_iam_role" "gtfs_data_fetch_scheduler_role" {
  name               = "gtfs-data-fetch-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role_policy.json
  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

data "aws_iam_policy_document" "gtfs_data_fetch_scheduler_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "lambda:InvokeFunction"
    ]

    resources = [
      aws_lambda_function.gtfs_data_fetch_lambda.arn
    ]
  }
}

resource "aws_iam_role_policy" "gtfs_data_fetch_scheduler_policy" {
  name   = "gtfs-data-fetch-scheduler-policy"
  role   = aws_iam_role.gtfs_data_fetch_scheduler_role.id
  policy = data.aws_iam_policy_document.gtfs_data_fetch_scheduler_policy_document.json
}

resource "aws_scheduler_schedule" "gtfs_data_fetch_schedule" {
  name                         = "gtfs-data-fetch-trigger"
  description                  = "Trigger GTFS data fetch every night at midnight CST"
  schedule_expression          = "cron(0 0 * * ? *)"
  schedule_expression_timezone = "America/Chicago"
  state                        = "ENABLED"
  
  flexible_time_window {
    mode = "OFF"
  }
  
  target {
    arn      = aws_lambda_function.gtfs_data_fetch_lambda.arn
    role_arn = aws_iam_role.gtfs_data_fetch_scheduler_role.arn
  }
}
