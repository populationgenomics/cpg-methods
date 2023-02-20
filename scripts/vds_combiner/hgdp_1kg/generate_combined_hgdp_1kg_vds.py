#!/usr/bin/env python3

"""Create combined HGDP + 1kG VDS"""

from cpg_utils.hail_batch import output_path
import hail as hl

HGDP = 'gs://cpg-hgdp-test/vds/1-0.vds'
ONEKG = 'gs://cpg-thousand-genomes-test/vds/1-0.vds'


def query():
    """Create combined HGDP + 1kG VDS, using the VDS combiner"""

    hl.init(default_reference='GRCh38')

    vdses = [
        'HGDP',
        'ONEKG'
    ]

    combiner = hl.vds.new_combiner(
        output_path=output_path('hgdp_1kg.vds'),
        temp_path=output_path('hgdp_1kg.vds', 'tmp'),
        vds_paths=vdses,
        use_genome_default_intervals=True,
    )

    combiner.run()


if __name__ == '__main__':
    query()
