output "cloudfront_domain_name" {
  description = "CloudFront distribution domain name (use as the octree base URL)."
  value       = aws_cloudfront_distribution.octree.domain_name
}

output "cloudfront_distribution_id" {
  description = "CloudFront distribution ID (for cache invalidation)."
  value       = aws_cloudfront_distribution.octree.id
}

output "bucket_name" {
  description = "S3 bucket name."
  value       = local.bucket_id
}

output "octree_base_url" {
  description = "HTTPS base URL for octree data via CloudFront."
  value       = "https://${aws_cloudfront_distribution.octree.domain_name}"
}
