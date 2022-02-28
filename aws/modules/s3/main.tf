resource "aws_s3_bucket" "bucket" {
  #bucket_prefix = var.bucket_prefix
  bucket = "raw-data-raquel"

  versioning {
    enabled = var.versioning
  }

  tags = {
    Name = "raw-data-raquel"
  }
}

resource "aws_s3_bucket" "bucket2" {
  
  #bucket_prefix = var.bucket_prefix
  bucket = "staging-raquel"
  versioning {
    enabled = var.versioning
  }

  tags = {
    Name = "stagin-raquel"
  }
}