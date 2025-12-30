terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.14.1"
    }
    github = {
      source  = "integrations/github"
      version = ">= 5.0"
    }
  }
}
