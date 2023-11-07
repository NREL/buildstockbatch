# Terraform file to define GCP resources used by buildstock_gcp
#
# When setting up a new project:
#   terraform init
#
# To see what changes will be applied:
#   terraform plan
#
# To apply those changes:
#   terraform apply
#
# Optionally set variables:
#   terraform apply -var="gcp_project=myproject" -var="bucket_name=mybucket" -var="region=us-east1-b"
#
# Format this file:
#   terraform fmt main.tf


variable "gcp_project" {
  type        = string
  default     = "buildstockbatch-dev"
  description = "GCP project to use"
}

variable "bucket_name" {
  type        = string
  default     = "buildstockbatch"
  description = "GCS bucket where buildstockbatch inputs and outputs should be stored"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "GCP region where all resources will be created"
}

variable "artifact_registry_repository" {
  type        = string
  default     = "buildstockbatch-docker"
  description = "Name of the artifact registry repository for storing Docker images. May contain letters, numbers, hyphens."
}


provider "google" {
  project = var.gcp_project
  region  = var.region
}

# GCS bucket for storing inputs and outputs
resource "google_storage_bucket" "bucket" {
  name                        = var.bucket_name
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true
}

# Artifact registry repository
resource "google_artifact_registry_repository" "AR_repo" {
  location      = var.region
  repository_id = var.artifact_registry_repository
  format        = "DOCKER"
}
