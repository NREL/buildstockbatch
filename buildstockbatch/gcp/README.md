# Buildstock Batch on GCP

![Architecture diagram](/buildstockbatch/gcp/arch.svg)

Buildstock Batch runs on GCP in a few phases:

  * Locally
    - Build a Docker image that includes OpenStudio and BuildStock Batch.
    - Push the Docker image to GCP Artifact Registry.
    - Run sampling and split the generated buildings + upgrades into batches.
    - Collect all the required input files (including downloading weather files)
      and upload them to a Cloud Storage bucket.
    - Create and start the Batch and Cloud Run jobs (described below),
      and wait for them to finish.

  * In GCP Batch
    - Run a batch job where each task runs a small group of simulations.
      GCP Batch uses the Docker image to run OpenStudio on Compute Engine VMs.
    - Raw output files are written to the bucket in Cloud Storage.

  * In Cloud Run
    - Run a job for post-processing steps. Also uses the Docker image.
    - Aggregated output files are written to the bucket in Cloud Storage.


`gcp.py` also supports validating a project file, cleaning up old projects,
and viewing the state of existing jobs.
