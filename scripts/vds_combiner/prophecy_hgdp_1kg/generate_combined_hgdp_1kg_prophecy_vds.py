#!/usr/bin/env python3

"""Create combined hgdp-1kG + PROPHECY VDS"""

from cpg_utils.hail_batch import output_path, dataset_path
import hail as hl

HGDP_ONEKG = dataset_path('vds/1-0.vds', dataset='hgdp-1kg')
PROPHECY = dataset_path('vds/1-1.vds', dataset='prophecy')


def query():
    """Create combined hgdp-1kG + PROPHECY VDS, using the VDS combiner"""

    hl.init(default_reference='GRCh38')

    combiner = hl.vds.new_combiner(
        output_path=output_path('1-0.vds'),
        temp_path=output_path('1-0.vds', 'tmp'),
        vds_paths=[HGDP_ONEKG, PROPHECY],
        use_genome_default_intervals=True,
    )

    combiner.run()


if __name__ == '__main__':
    query()
