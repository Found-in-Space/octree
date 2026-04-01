# Infrastructure — CloudFront CDN for octree data

Terraform config that puts a CloudFront distribution in front of the S3 bucket
where octree files are stored. This gives browsers HTTP/2 multiplexing, edge
caching, and eliminates the HTTP/1.1 six-connection limit that makes range
request–heavy workloads slow.

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5
- AWS credentials configured (`aws configure` or environment variables)

## Quick start

```bash
cd infra/

# 1. Create your variables file from the example
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — set bucket_name, create_bucket, region, etc.

# 2. Initialise and apply
terraform init
terraform plan      # review what will be created
terraform apply     # create the CloudFront distribution
```

After apply, Terraform prints the CloudFront domain:

```
octree_base_url = "https://d1234abcdef8.cloudfront.net"
```

Use that as the base URL for octree files, e.g.:

```
https://d1234abcdef8.cloudfront.net/c56103e6-ad4c-41f9-be06-048b48ec632b/stars.octree
```

## Existing vs new bucket

Set `create_bucket = false` (the default) to use an existing S3 bucket.
Terraform reads the bucket as a data source and adds a bucket policy granting
CloudFront read access via Origin Access Control.

Set `create_bucket = true` to have Terraform create the bucket. It will also
configure public access blocking and CORS.

## What gets created

| Resource | Purpose |
|----------|---------|
| CloudFront distribution | HTTP/2+3 CDN, edge caching |
| Origin Access Control | Lets CloudFront read private S3 objects |
| S3 bucket policy | Grants CloudFront read access |
| Cache policy | Includes `Range` header in cache key so range requests cache correctly |
| Response headers policy | CORS headers for browser range requests |
| S3 bucket *(optional)* | Only when `create_bucket = true` |

## Cache invalidation

Octree files are addressed by dataset UUID, so new builds get new paths
and don't need invalidation. If you do need to invalidate:

```bash
aws cloudfront create-invalidation \
  --distribution-id $(terraform output -raw cloudfront_distribution_id) \
  --paths "/*"
```

## Destroying

```bash
terraform destroy
```

This removes the CloudFront distribution, policies, and OAC. If
`create_bucket = true`, it also removes the S3 bucket (which must be empty).
