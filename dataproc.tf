terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~>6.0.0"
    }
  }
}

provider "google" {
  credentials = file("./roam-dataproc.json")
  project     = var.project
  region      = var.regiob
}


resource "google_dataproc_cluster" "etl_cluster" {
  name     = var.cluster_name
  provider = google
  region   = var.region
  labels   = var.labels

  cluster_config {
    staging_bucket = var.staging_bucket

    endpoint_config {
      enable_http_port_access = "true"
    }

    master_config {
      num_instances = 1
      machine_type  = "n2-standard-4"

      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 50
        num_local_ssds    = 0
      }
    }

    worker_config {
      num_instances = 2
      machine_type  = "n2-standard-4"

      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 50
        num_local_ssds    = 0
      }
    }


    preemptible_worker_config {
      num_instances = 0
    }

    gce_cluster_config {
      tags = [var.cluster_name]
      zone = var.zone
    }
    software_config {
      image_version       = "2.1-debian11"
      optional_components = ["JUPYTER"]
    }
  }
}
