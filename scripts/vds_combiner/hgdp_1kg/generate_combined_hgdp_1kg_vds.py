#!/usr/bin/env python3

"""Create combined HGDP + 1kG VDS"""

from cpg_utils.hail_batch import output_path, dataset_path
import hail as hl

HGDP = dataset_path('vds/1-0.vds', dataset='hgdp')
ONEKG = dataset_path('vds/1-0.vds', dataset='thousand-genomes')


def query():
    """Create combined HGDP + 1kG VDS, using the VDS combiner"""

    hl.init(default_reference='GRCh38')

    vdses = [HGDP, ONEKG]

    combiner = hl.vds.new_combiner(
        output_path=output_path('hgdp_1kg.vds'),
        temp_path=output_path('hgdp_1kg.vds', 'tmp'),
        vds_paths=vdses,
        use_genome_default_intervals=True,
    )

    combiner.run()


if __name__ == '__main__':
    query()
