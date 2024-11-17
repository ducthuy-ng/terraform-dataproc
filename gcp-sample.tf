terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
  backend "gcs" {
    bucket = "spark-artifacts"
  }
}

provider "google" {
  project = "modified-glyph-438213-k8"
}

resource "google_storage_bucket" "staging-spark" {
  location = "asia-southeast1"
  name     = "staging-spark"
}

resource "google_compute_network" "spark_network" {
  name = "spark-network"
}

resource "google_compute_firewall" "spark_network_firewall" {
  name    = "spark-network-firewall"
  network = google_compute_network.spark_network.name

  allow {
    protocol = "all"
  }

  source_ranges = ["192.168.0.0/16"]
}

resource "google_compute_subnetwork" "a_private_google_access_subnet" {
  name                     = "a-private-google-access-subnet"
  region                   = "asia-southeast1"
  network                  = google_compute_network.spark_network.name
  ip_cidr_range            = "192.168.0.0/16"
  private_ip_google_access = true
}

resource "google_bigquery_dataset" "data_warehouse" {
  dataset_id    = "spark_retailer_dataset_workaround"
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
        boot_disk_size_gb = 50
      }
    }

    worker_config {
      num_instances = 2
      machine_type  = "n2-standard-2"
      disk_config {
        boot_disk_type    = "pd-balanced"
        boot_disk_size_gb = 50
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
