"""Entry point for the analysis runner."""

import hailtop.batch as hb
from analysis_runner import dataproc
from cpg_utils.hail_batch import get_config, remote_tmpdir

config = get_config()

service_backend = hb.ServiceBackend(
    billing_project=config['hail']['billing_project'],
    remote_tmpdir=remote_tmpdir(),
)

batch = hb.Batch(name='hgdp1kg-prophecy-vds', backend=service_backend)

dataproc.hail_dataproc_job(
    batch,
    'generate_combined_hgdp_1kg_prophecy_vds.py',
    max_age='12h',
    num_secondary_workers=20,
    init=['gs://cpg-common-main/hail_dataproc/install_common.sh'],
    job_name='hgdp1kg_prophecy_vds',
)

batch.run()
