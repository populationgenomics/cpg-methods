#!/usr/bin/env python3

"""Pipeline for choosing markers for datasets, using an input VDS dataset
and the HGDP/1kG datasets"""

from cpg_utils.hail_batch import output_path, dataset_path
import hail as hl
import click

HGDP_ONEKG = dataset_path('vds/v1-0.vds', dataset='hgdp-1kg')
NUM_ROWS_BEFORE_LD_PRUNE = 200000

@click.command()
@click.option('--path', help='VDS dataset path, without the gs://cpg-{dataset}-{access-level} prefix', required=True)
@click.option('--output-version', help='Version of dataset made by VDS combiner, e.g., v1-0', required=True)
def main(path, output_version):
    """ Create a sites table using a representative subset of variants based off of 
    the following criteria:
    Sites are biallelic, autosomal, have an allele frequency above 1%, have a call rate above 99%, 
    and have an inbreeding coefficient > -0.25.
    """

    hl.init(default_reference='GRCh38')

    INPUT_DATASET = dataset_path(path)
    COMBINED_DATASET = output_path(f'{output_version}.vds')

    # extract only relevant entries so hgdp/1kg and input datasets are the same
    # only LGT and LA entries are needed to calculate the GT field
    # The GT field is used downstream for the variant_qc() function
    hgdp_onekg = hl.vds.read_vds(HGDP_ONEKG)
    input_dataset = hl.vds.read_vds(INPUT_DATASET)
    hgdp_onekg.variant_data = hgdp_onekg.variant_data.select_entries(hgdp_onekg.variant_data.LGT, hgdp_onekg.variant_data.LA)
    input_dataset.variant_data = input_dataset.variant_data.select_entries(input_dataset.variant_data.LGT, input_dataset.variant_data.LA)
    # save datasets to tmp bucket
    tmp_hgdp_onekg_output = output_path(f'filtered_entries_hgdp_1kg.vds', 'tmp')
    tmp_input_dataset_output = output_path(f'filtered_entries_{output_version}.vds', 'tmp')
    hgdp_onekg.write(tmp_hgdp_onekg_output)
    input_dataset.write(tmp_input_dataset_output)

    combiner = hl.vds.new_combiner(
        output_path=output_path(f'{output_version}.vds'),
        temp_path=output_path(f'{output_version}.vds', 'tmp'),
        vds_paths=[tmp_hgdp_onekg_output, tmp_input_dataset_output],
        use_genome_default_intervals=True,
    )

    combiner.run()

    vds = hl.vds.read_vds(COMBINED_DATASET)

    # split multiallelics, then densify
    vds = hl.vds.split_multi(vds, filter_changed_loci=True)
    mt = hl.vds.to_dense_mt(vds)

    # run variant QC
    mt = hl.variant_qc(mt)
    # choose variants based off of gnomAD v3 parameters
    # Inbreeding coefficient > -0.25 (no excess of heterozygotes)
    # Must be single nucleotide variants that are autosomal (i.e., no sex), and bi-allelic
    # Have an allele frequency above 1% (note deviation from gnomAD, which is 0.1%)
    # Have a call rate above 99%
    mt = mt.annotate_rows(
        IB=hl.agg.inbreeding(
            mt.GT, mt.variant_qc.AF[1]
        )
    )
    mt = mt.filter_rows(
        (hl.len(mt.alleles) == 2)
        & (mt.locus.in_autosome())
        & (mt.variant_qc.AF[1] > 0.01)
        & (mt.variant_qc.call_rate > 0.99)
        & (mt.IB.f_stat > -0.25)
    )

    checkpoint_path = output_path('hgdp_1kg_prophecy_pre_pruning.mt', 'tmp')
    mt = mt.checkpoint(checkpoint_path, overwrite=True)
    nrows = mt.count_rows()
    print(f'mt.count_rows() = {nrows}')
    # downsize input variants for ld_prune
    # otherwise, persisting the pruned_variant_table will cause
    # script to fail. See https://github.com/populationgenomics/ancestry/pull/79
    mt = mt.sample_rows(
        NUM_ROWS_BEFORE_LD_PRUNE / nrows, seed=12345
    )

    # as per gnomAD, LD-prune variants with a cutoff of r2 = 0.1
    pruned_variant_table = hl.ld_prune(
        mt.GT, r2=0.1, bp_window_size=500000
    )
    pruned_variant_table_path = dataset_path('pruned_variants.ht')
    pruned_variant_table.write(pruned_variant_table_path)


if __name__ == '__main__':
    main()
