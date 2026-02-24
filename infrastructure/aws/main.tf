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

  statement {
    effect = "Allow"

    actions = [
      "sns:Publish"
    ]

    resources = [
      aws_sns_topic.lambda_status_execution_updates.arn
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
  handler                        = "main.handler"
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

resource "aws_lambda_function_event_invoke_config" "gtfs_data_fetch_status_sns" {
  function_name = aws_lambda_function.gtfs_data_fetch_lambda.function_name

  destination_config {
    on_failure {
      destination = aws_sns_topic.lambda_status_execution_updates.arn
    }
    on_success {
      destination = aws_sns_topic.lambda_status_execution_updates.arn
    }
  }
}

###########################################################################
########## SNS Topic for Lambda Execution Status Notifications ###########
###########################################################################
resource "aws_sns_topic" "lambda_status_execution_updates" {
  name = "lambda-status-execution-updates"

  tags = {
    Project     = "cta-analytics-app"
    Environment = "PROD"
  }
}

resource "aws_sns_topic_subscription" "lambda_execution_status_email_subscription" {
  topic_arn = aws_sns_topic.lambda_status_execution_updates.arn
  protocol  = "email"
  endpoint  = var.notification_email
}

###########################################################################
###################### GTFS Expected Schedule Lambda ######################
###########################################################################
resource "aws_iam_role" "gtfs_expected_schedule_role" {
  name               = "gtfs-expected-schedule-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role_policy.json
  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

data "aws_iam_policy_document" "gtfs_expected_schedule_lambda_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "s3:PutObject",
      "s3:GetObject"
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
      "arn:aws:logs:us-east-1:${local.account_id}:log-group:/aws/lambda/gtfs-expected-schedule:*"
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "sns:Publish"
    ]

    resources = [
      aws_sns_topic.lambda_status_execution_updates.arn
    ]
  }
}

resource "aws_iam_role_policy" "gtfs_expected_schedule_lambda_policy" {
  name   = "gtfs-expected-schedule-lambda-policy"
  role   = aws_iam_role.gtfs_expected_schedule_role.id
  policy = data.aws_iam_policy_document.gtfs_expected_schedule_lambda_policy_document.json
}

resource "aws_lambda_function" "gtfs_expected_schedule_lambda" {
  function_name                  = "gtfs-expected-schedule"
  description                    = "Lambda function to create CTA expected schedule from GTFS data"
  role                           = aws_iam_role.gtfs_expected_schedule_role.arn
  handler                        = "main.handler"
  runtime                        = "python3.13"
  filename                       = "../../lambdas/gtfs_expected_schedule/deployment_package.zip"
  source_code_hash               = filebase64sha256("../../lambdas/gtfs_expected_schedule/deployment_package.zip")
  timeout                        = 60
  memory_size                    = 2048
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

resource "aws_lambda_function_event_invoke_config" "gtfs_expected_schedule_invoke_config" {
  function_name = aws_lambda_function.gtfs_expected_schedule_lambda.function_name

  destination_config {
    on_failure {
      destination = aws_sns_topic.lambda_status_execution_updates.arn
    }
    on_success {
      destination = aws_sns_topic.lambda_status_execution_updates.arn
    }
  }
}

###########################################################################
######################### Lambda Orchestrator #############################
###########################################################################
data "aws_iam_policy_document" "sfn_assume_role_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole"
    ]
  }
}

resource "aws_iam_role" "step_functions_role" {
  name = "gtfs-lambda-orchestrator-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume_role_policy.json
  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

data "aws_iam_policy_document" "gtfs_sfn_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "lambda:InvokeFunction"
    ]

    resources = [
      aws_lambda_function.gtfs_data_fetch_lambda.arn,
      aws_lambda_function.gtfs_expected_schedule_lambda.arn
    ]
  }
}

resource "aws_iam_role_policy" "gtfs_sfn_policy" {
  name   = "gtfs-sfn-policy"
  role   = aws_iam_role.step_functions_role.id
  policy = data.aws_iam_policy_document.gtfs_sfn_policy_document.json
}

resource "aws_sfn_state_machine" "gtfs_lambda_orchestrator" {
  name     = "gtfs-lambda-orchestrator"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    StartAt = "Lambda1"
    States = {
      Lambda1 = {
        Type     = "Task"
        Resource = aws_lambda_function.gtfs_data_fetch_lambda.arn
        Next     = "FileCheckChoice"
      }
      FileCheckChoice = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.status"
            StringEquals = "updated"
            Next         = "Lambda2"
          }
        ]
        Default = "Done"
      }
      Lambda2 = {
        Type     = "Task"
        Resource = aws_lambda_function.gtfs_expected_schedule_lambda.arn
        End      = true
      }
      Done = {
        Type = "Succeed"
      }
    }
  })

  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

###########################################################################
######################## GTFS Data SFN Scheduler ##########################
###########################################################################

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
      "states:StartExecution"
    ]

    resources = [
      aws_sfn_state_machine.gtfs_lambda_orchestrator.arn
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
    arn      = aws_sfn_state_machine.gtfs_lambda_orchestrator.arn
    role_arn = aws_iam_role.gtfs_data_fetch_scheduler_role.arn
  }
}
