terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = "modified-glyph-438213-k8"
}

resource "google_compute_network" "a_working_network" {
  name = "a-working-network"

}

resource "google_compute_subnetwork" "a_private_google_access_subnet" {
  name                     = "a-private-google-access-subnet"
  region                   = "asia-southeast1"
  network                  = "a-working-network"
  ip_cidr_range            = "192.168.0.0/16"
  private_ip_google_access = true
}

resource "google_bigquery_dataset" "data_warehouse" {
  dataset_id    = "spark_retailer_dataset"
  friendly_name = "Retailer Raw Files"
  location      = "asia-southeast1"
}

resource "google_dataproc_cluster" "dataproc_cluster" {
  name                          = "dataproc-cluster"
  region                        = "asia-southeast1"
  graceful_decommission_timeout = "120s"

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n2-standard-2"
      disk_config {
        boot_disk_type    = "pd-balanced"
        boot_disk_size_gb = 100
      }
    }

    worker_config {
      num_instances = 0
      machine_type  = "n2-standard-2"
      disk_config {
        boot_disk_type    = "pd-balanced"
        boot_disk_size_gb = 100
      }
    }

    preemptible_worker_config {
      num_instances = 0
    }

    software_config {
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    gce_cluster_config {
      subnetwork = google_compute_subnetwork.a_private_google_access_subnet.name
    }
  }
}
