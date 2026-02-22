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

# S3 bucket to hold state for rest of project
resource "aws_s3_bucket" "state_bucket" {
  bucket = "${data.aws_caller_identity.current.account_id}-cta-analytics-terraform-state-bucket"

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

# Set versioning to remove noncurrent tf.state files in this bucket
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
