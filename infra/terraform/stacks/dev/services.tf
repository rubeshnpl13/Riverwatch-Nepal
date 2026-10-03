locals {
  required_services = toset([
    # Identity and project management
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",

    # Container build/runtime
    "artifactregistry.googleapis.com",
    "run.googleapis.com",

    # Event-driven pipeline
    "cloudscheduler.googleapis.com",
    "pubsub.googleapis.com",

    # Data lake
    "storage.googleapis.com",

    # Processing
    "compute.googleapis.com",
    "dataproc.googleapis.com",

    # Analytics
    "bigquery.googleapis.com",
    "bigqueryconnection.googleapis.com",

    # Observability
    "logging.googleapis.com",
    "monitoring.googleapis.com",
  ])
}


module "project_services" {
  source = "../../modules/project_services"

  project_id = var.project_id
  services   = local.required_services
}