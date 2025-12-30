terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.45.0"
    }
    github = {
      source  = "integrations/github"
      version = ">= 5.0"
    }
  }
}
