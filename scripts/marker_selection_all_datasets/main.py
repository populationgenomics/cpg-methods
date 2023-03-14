"""Entry point for the analysis runner."""

import click
import hailtop.batch as hb
from analysis_runner import dataproc
from cpg_utils.hail_batch import get_config, remote_tmpdir

@click.command()
@click.option('--vds-path', help='VDS dataset path, without the gs://cpg-{dataset}-{access-level} prefix', required=True)
@click.option('--output-version', help='Version of dataset made by VDS combiner, e.g., 1-0', required=True)
def main(vds_path: str, output_version: str):
    """
    runs a script inside dataproc to execute the marker selection
    """

    config = get_config()

    service_backend = hb.ServiceBackend(
        billing_project=config['hail']['billing_project'],
        remote_tmpdir=remote_tmpdir(),
    )

    batch = hb.Batch(name='marker-selection', backend=service_backend)

    dataproc.hail_dataproc_job(
        batch,
        script=f'marker_selection_hgdp_1kg_all_datasets.py --vds-path {vds_path} --output-version {output_version}',
        max_age='6h',
        num_secondary_workers=20,
        init=['gs://cpg-common-main/hail_dataproc/install_common.sh'],
        job_name='marker_selection',
    )

    batch.run()

if __name__ == '__main__':
    main()  # pylint: disable=E1120