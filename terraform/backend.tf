terraform {
  backend "gcs" {
    bucket  = "raspberry-9dcf6-tfstate"
    prefix  = "terraform/state/portfolio-pipeline"
  }
}