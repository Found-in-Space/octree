terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------------
# S3 bucket — create or reference existing
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "octree" {
  count  = var.create_bucket ? 1 : 0
  bucket = var.bucket_name
}

data "aws_s3_bucket" "existing" {
  count  = var.create_bucket ? 0 : 1
  bucket = var.bucket_name
}

locals {
  bucket_id              = var.create_bucket ? aws_s3_bucket.octree[0].id : data.aws_s3_bucket.existing[0].id
  bucket_arn             = var.create_bucket ? aws_s3_bucket.octree[0].arn : data.aws_s3_bucket.existing[0].arn
  bucket_regional_domain = var.create_bucket ? aws_s3_bucket.octree[0].bucket_regional_domain_name : data.aws_s3_bucket.existing[0].bucket_regional_domain_name
}

# Block public access on managed buckets (existing buckets keep their config).
resource "aws_s3_bucket_public_access_block" "octree" {
  count  = var.create_bucket ? 1 : 0
  bucket = aws_s3_bucket.octree[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# CORS — needed so the browser can send Range headers cross-origin.
resource "aws_s3_bucket_cors_configuration" "octree" {
  count  = var.create_bucket ? 1 : 0
  bucket = aws_s3_bucket.octree[0].id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]
    expose_headers  = ["Content-Range", "Content-Length", "ETag"]
    max_age_seconds = 86400
  }
}

# ---------------------------------------------------------------------------
# CloudFront Origin Access Control
# ---------------------------------------------------------------------------

resource "aws_cloudfront_origin_access_control" "octree" {
  name                              = "${var.bucket_name}-oac"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# ---------------------------------------------------------------------------
# S3 bucket policy — allow CloudFront OAC to read objects
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_policy" "cloudfront_read" {
  bucket = local.bucket_id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowCloudFrontServicePrincipalReadOnly"
        Effect    = "Allow"
        Principal = { Service = "cloudfront.amazonaws.com" }
        Action    = "s3:GetObject"
        Resource  = "${local.bucket_arn}/*"
        Condition = {
          StringEquals = {
            "AWS:SourceArn" = aws_cloudfront_distribution.octree.arn
          }
        }
      },
    ]
  })
}

# ---------------------------------------------------------------------------
# CloudFront cache policy — include Range header in cache key
# ---------------------------------------------------------------------------

resource "aws_cloudfront_cache_policy" "range_aware" {
  name        = "${var.bucket_name}-range-aware"
  min_ttl     = 86400
  default_ttl = 604800
  max_ttl     = 31536000
  comment     = "Cache octree range requests — Range header in cache key."

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "none"
    }
    headers_config {
      header_behavior = "whitelist"
      headers {
        items = ["Range"]
      }
    }
    enable_accept_encoding_brotli = false
    enable_accept_encoding_gzip  = false
  }
}

# ---------------------------------------------------------------------------
# CloudFront response headers policy — CORS for browser range requests
# ---------------------------------------------------------------------------

resource "aws_cloudfront_response_headers_policy" "cors" {
  name    = "${var.bucket_name}-cors"
  comment = "CORS headers for octree range requests."

  cors_config {
    access_control_allow_credentials = false

    access_control_allow_headers {
      items = ["Range"]
    }

    access_control_allow_methods {
      items = ["GET", "HEAD"]
    }

    access_control_allow_origins {
      items = ["*"]
    }

    access_control_expose_headers {
      items = ["Content-Range", "Content-Length", "ETag", "Accept-Ranges"]
    }

    access_control_max_age_sec = 86400
    origin_override            = true
  }
}

# ---------------------------------------------------------------------------
# CloudFront distribution
# ---------------------------------------------------------------------------

resource "aws_cloudfront_distribution" "octree" {
  comment             = var.cloudfront_comment
  enabled             = true
  is_ipv6_enabled     = true
  http_version        = "http2and3"
  price_class         = var.cloudfront_price_class
  wait_for_deployment = false

  origin {
    domain_name              = local.bucket_regional_domain
    origin_id                = "s3-octree"
    origin_access_control_id = aws_cloudfront_origin_access_control.octree.id
  }

  default_cache_behavior {
    target_origin_id             = "s3-octree"
    viewer_protocol_policy       = "redirect-to-https"
    allowed_methods              = ["GET", "HEAD", "OPTIONS"]
    cached_methods               = ["GET", "HEAD"]
    cache_policy_id              = aws_cloudfront_cache_policy.range_aware.id
    response_headers_policy_id   = aws_cloudfront_response_headers_policy.cors.id
    compress                     = false
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}
