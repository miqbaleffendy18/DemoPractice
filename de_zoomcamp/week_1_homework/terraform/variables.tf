variable "credentials" {
  default = "./keys/gcp-creds.json"
}

variable "project_id" {
  default = "de-zoomcamp-422823"
}

variable "region" {
  description = "Project Region"
  default     = "asia-southeast2-a"
}

variable "location" {
  description = "Project Location"
  default     = "ASIA-SOUTHEAST2"
}

variable "bq_dataset_name" {
  description = "Demo Dataset Big Query Name"
  default     = "demo_dataset"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "Bucket Storage Name"
  default     = "de-zoomcamp-422823-demo-bucket"
}