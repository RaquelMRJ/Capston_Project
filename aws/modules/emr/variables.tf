variable "name" {
  description = "name"
  type        = string
}
variable "release_label" {
  description = "release"
  type        = string
}
variable "subnet_id" {
  description = "Subnet ID"
  type        = string
}
#variable "bootstrap_action {path="s3://bucket/bootstrap/hive/metastore/JSON41.sh",name="addPackages"}
