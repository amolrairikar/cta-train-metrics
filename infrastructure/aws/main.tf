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
resource "aws_s3_bucket" "application_bucket" {
  bucket = "${local.account_id}-cta-analytics-project"

  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

resource "aws_s3_bucket_versioning" "state_bucket_versioning" {
  bucket = aws_s3_bucket.application_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "s3_public_access_block" {
  bucket = aws_s3_bucket.application_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Set versioning to remove noncurrent files in this bucket
resource "aws_s3_bucket_lifecycle_configuration" "s3_expire_old_versions" {
  depends_on = [aws_s3_bucket_versioning.state_bucket_versioning]

  bucket = aws_s3_bucket.application_bucket.id

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

  rule {
    id     = "expire_raw_api_data"
    status = "Enabled"

    filter {
      prefix = "raw-api-data/"
    }

    expiration {
      days = 7
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

  statement {
    effect = "Allow"

    actions = [
      "sns:Publish"
    ]

    resources = [
      aws_sns_topic.lambda_orchestrator_execution_updates.arn
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

  definition = jsonencode(
    {
      "StartAt": "Lambda1",
      "States": {
        "Lambda1": {
          "Type": "Task",
          "Resource": "arn:aws:states:::lambda:invoke",
          "Parameters": {
            "FunctionName": "${aws_lambda_function.gtfs_data_fetch_lambda.arn}"
          },
          "Catch": [{
            "ErrorEquals": ["States.ALL"],
            "Next": "NotifyFailure"
          }],
          "Next": "FileCheckChoice"
        },
        "FileCheckChoice": {
          "Type": "Choice",
          "Choices": [{
            "Variable": "$.Payload.status", 
            "StringEquals": "updated",
            "Next": "Lambda2"
          }],
          "Default": "Done"
        },
        "Lambda2": {
          "Type": "Task",
          "Resource": "arn:aws:states:::lambda:invoke",
          "Parameters": {
            "FunctionName": "${aws_lambda_function.gtfs_expected_schedule_lambda.arn}"
          },
          "Catch": [{
            "ErrorEquals": ["States.ALL"],
            "Next": "NotifyFailure"
          }],
          "End": true
        },
        "NotifyFailure": {
          "Type": "Task",
          "Resource": "arn:aws:states:::sns:publish",
          "Parameters": {
            "TopicArn": "${aws_sns_topic.lambda_orchestrator_execution_updates.arn}",
            "Message": "The GTFS pipeline failed in the Step Function.",
            "Subject": "Alert: GTFS Pipeline Failure"
          },
          "End": true
        },
        "Done": {
          "Type": "Succeed"
        }
      }
    }
  )

  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

###########################################################################
############ SNS Topic for Lambda Orchestrator Notifications ##############
###########################################################################
resource "aws_sns_topic" "lambda_orchestrator_execution_updates" {
  name = "lambda-orchestrator-execution-updates"

  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

resource "aws_sns_topic_subscription" "lambda_orchestrator_status_email_subscription" {
  topic_arn = aws_sns_topic.lambda_orchestrator_execution_updates.arn
  protocol  = "email"
  endpoint  = var.notification_email
}

###########################################################################
#################### SQS Queue for End-to-End Testing #####################
###########################################################################

resource "aws_sqs_queue" "end_to_end_testing_queue" {
  name      = "e2e-testing-queue"
  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

resource "aws_sqs_queue_policy" "example" {
  queue_url = aws_sqs_queue.end_to_end_testing_queue.id

  policy = jsonencode({
    Version = "2012-10-17" # !! Important !!
    Statement = [{
      Sid    = "Allow-SNS-SendMessage"
      Effect = "Allow"

      Principal = {
        Service = "sns.amazonaws.com"
      }
      Action   = "SQS:SendMessage"
      Resource = aws_sqs_queue.end_to_end_testing_queue.arn

      Condition = {
        ArnLike = {
          "aws:SourceArn" = aws_sns_topic.lambda_orchestrator_execution_updates.arn
        }
      }
    }]
  })
}

resource "aws_sns_topic_subscription" "e2e_testing_subscription" {
  topic_arn = aws_sns_topic.lambda_orchestrator_execution_updates.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.end_to_end_testing_queue.arn
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

###########################################################################
######################### Train Locations Lambda ##########################
###########################################################################
resource "aws_iam_role" "cta_get_train_locations_role" {
  name               = "cta-get-train-locations-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role_policy.json
  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

data "aws_iam_policy_document" "cta_get_train_locations_lambda_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "firehose:PutRecord"
    ]

    resources = [
      aws_kinesis_firehose_delivery_stream.cta_train_locations_stream.arn
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
      "arn:aws:logs:us-east-1:${local.account_id}:log-group:/aws/lambda/cta-get-train-locations:*",
      "arn:aws:logs:us-east-1:${local.account_id}:log-group:/aws/lambda/cta-get-train-locations-test:*"
    ]
  }
}

resource "aws_iam_role_policy" "cta_get_train_locations_lambda_policy" {
  name   = "cta-get-train-locations-lambda-policy"
  role   = aws_iam_role.cta_get_train_locations_role.id
  policy = data.aws_iam_policy_document.cta_get_train_locations_lambda_policy_document.json
}

resource "aws_lambda_function" "cta_get_train_locations_lambda" {
  function_name                  = "cta-get-train-locations"
  description                    = "Lambda function to make CTA API request to fetch train locations"
  role                           = aws_iam_role.cta_get_train_locations_role.arn
  handler                        = "main.handler"
  runtime                        = "python3.13"
  filename                       = "../../lambdas/train_location_fetch/deployment_package.zip"
  source_code_hash               = filebase64sha256("../../lambdas/train_location_fetch/deployment_package.zip")
  timeout                        = 60
  memory_size                    = 256
  environment {
    variables = {
      CTA_API_KEY = var.api_key
    }
  }
  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

resource "aws_lambda_function" "cta_get_train_locations_test_lambda" {
  function_name                  = "cta-get-train-locations-test"
  description                    = "Lambda function used for testing to make CTA API request to fetch train locations"
  role                           = aws_iam_role.cta_get_train_locations_role.arn
  handler                        = "main.handler"
  runtime                        = "python3.13"
  filename                       = "../../lambdas/train_location_fetch/deployment_package.zip"
  source_code_hash               = filebase64sha256("../../lambdas/train_location_fetch/deployment_package.zip")
  timeout                        = 60
  memory_size                    = 256
  environment {
    variables = {
      CTA_API_KEY = var.api_key
    }
  }
  tags = {
    Project     = "cta-train-metrics"
    Environment = "DEV"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_error_alarm" {
  alarm_name          = "train-location-lambda-error-threshold-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 600  # 10 minutes
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "This alarm monitors lambda errors and triggers if they exceed 5 in 10 minutes."
  
  # Map the alarm to your specific function
  dimensions = {
    FunctionName = "cta-get-train-locations"
  }

  # Action to take when alarm state is reached
  alarm_actions = [aws_sns_topic.lambda_orchestrator_execution_updates.arn]
}

###########################################################################
######################### Eventbridge Scheduler ###########################
###########################################################################
resource "aws_iam_role" "get_train_locations_eventbridge_role" {
  name               = "cta-get-train-locations-eventbridge-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role_policy.json
  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

data "aws_iam_policy_document" "get_train_locations_eventbridge_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "lambda:InvokeFunction"
    ]

    resources = [
      "arn:aws:lambda:us-east-1:${local.account_id}:function:cta-get-train-locations"
    ]
  }
}

resource "aws_iam_role_policy" "cta_get_train_locations_eventbridge_role_policy" {
  name   = "cta-get-train-locations-eventbridge-role-policy"
  role   = aws_iam_role.get_train_locations_eventbridge_role.id
  policy = data.aws_iam_policy_document.get_train_locations_eventbridge_policy_document.json
}

resource "aws_scheduler_schedule" "cta_get_train_locations_trigger" {
  name                         = "cta-get-train-locations-trigger"
  group_name                   = "default"
  state                        = "ENABLED"
  schedule_expression          = "rate(1 minute)"
  schedule_expression_timezone = "America/Chicago"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.cta_get_train_locations_lambda.arn
    role_arn = aws_iam_role.get_train_locations_eventbridge_role.arn
  }
}

###########################################################################
######################## Firehose Delivery Stream #########################
###########################################################################
data "aws_iam_policy_document" "firehose_assume_role_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole"
    ]
  }
}

resource "aws_iam_role" "cta_firehose_role" {
  name               = "cta-firehose-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume_role_policy.json
  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}

data "aws_iam_policy_document" "cta_firehose_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject"
    ]

    resources = [
      aws_s3_bucket.application_bucket.arn,
      "${aws_s3_bucket.application_bucket.arn}/*"
    ]
  }
}

resource "aws_iam_role_policy" "cta_firehose_policy" {
  name   = "cta-firehose-policy"
  role   = aws_iam_role.cta_firehose_role.id
  policy = data.aws_iam_policy_document.cta_firehose_policy_document.json
}

resource "aws_kinesis_firehose_delivery_stream" "cta_train_locations_stream" {
  name        = "cta-train-locations-stream"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn            = aws_iam_role.cta_firehose_role.arn
    bucket_arn          = aws_s3_bucket.application_bucket.arn

    # Buffer conditions: 
    # Firehose will flush to S3 if EITHER 128MB is reached OR 900 seconds (15 minutes) have passed.
    buffering_size      = 128
    buffering_interval  = 900

    # Enable compression to save on storage costs
    compression_format  = "GZIP"

    # Add prefixing for better S3 organization (e.g., date-based partitioning)
    prefix              = "raw-api-data/success/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    error_output_prefix = "raw-api-data/errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
  }

  tags = {
    Project     = "cta-train-metrics"
    Environment = "PROD"
  }
}
