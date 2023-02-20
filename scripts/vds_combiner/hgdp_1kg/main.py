"""Entry point for the analysis runner."""

import os
import hailtop.batch as hb
from analysis_runner import dataproc

service_backend = hb.ServiceBackend(
    billing_project=os.getenv("HAIL_BILLING_PROJECT"), bucket=os.getenv("HAIL_BUCKET")
)

batch = hb.Batch(name="hgdp-1kg-vds", backend=service_backend)

dataproc.hail_dataproc_job(
    batch,
    "generate_combined_hgdp_1kg_vds.py",
    max_age="2h",
    init=["gs://cpg-common-main/hail_dataproc/install_common.sh"],
    job_name=f"hgdp_1kg_vds",
)

batch.run()
