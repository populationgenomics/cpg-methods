#!/usr/bin/env python3

"""Pipeline for choosing markers for HGDP/1kG datasets"""

from cpg_utils.hail_batch import output_path, dataset_path
import hail as hl

HGDP_1KG = dataset_path('vds/1-0.vds')

NUM_ROWS_BEFORE_LD_PRUNE = 200000


def main():
    """choosing markers for HGDP/1kG datasets according to gnomAD"""

    hl.init(default_reference='GRCh38')

    vds = hl.vds.read_vds(HGDP_1KG)

    # split multiallelics, then densify
    vds = hl.vds.split_multi(vds, filter_changed_loci=True)
    hgdp_1kg = hl.vds.to_dense_mt(vds)

    # run variant QC
    hgdp_1kg = hl.variant_qc(hgdp_1kg)
    # choose variants based off of gnomAD v3 parameters
    # Inbreeding coefficient > -0.25 (no excess of heterozygotes)
    # Must be single nucleotide variants that are autosomal (i.e., no sex), and bi-allelic
    # Have an allele frequency above 1% (note deviation from gnomAD, which is 0.1%)
    # Have a call rate above 99%
    hgdp_1kg = hgdp_1kg.annotate_rows(
        IB=hl.agg.inbreeding(
            hgdp_1kg.GT, hgdp_1kg.variant_qc.AF[1]
        )
    )
    hgdp_1kg = hgdp_1kg.filter_rows(
        (hl.len(hgdp_1kg.alleles) == 2)
        & (hgdp_1kg.locus.in_autosome())
        & (hgdp_1kg.variant_qc.AF[1] > 0.01)
        & (hgdp_1kg.variant_qc.call_rate > 0.99)
        & (hgdp_1kg.IB.f_stat > -0.25)
    )

    # downsize input variants for ld_prune
    # otherwise, persisting the pruned_variant_table will cause
    # script to fail. See https://github.com/populationgenomics/ancestry/pull/79
    checkpoint_path = output_path('hgdp_1kg_pre_pruning.mt', 'tmp')
    hgdp_1kg = hgdp_1kg.checkpoint(checkpoint_path, overwrite=True)

    nrows = hgdp_1kg.count_rows()
    print(f'hgdp_1kg.count_rows() = {nrows}')
    hgdp_1kg = hgdp_1kg.sample_rows(
        NUM_ROWS_BEFORE_LD_PRUNE / nrows, seed=12345
    )

    # as per gnomAD, LD-prune variants with a cutoff of r2 = 0.1
    pruned_variant_table = hl.ld_prune(
        hgdp_1kg.GT, r2=0.1, bp_window_size=500000
    )
    # repartition table after pruning
    pruned_variant_table = pruned_variant_table.repartition(100, shuffle=False)
    pruned_variant_table_path = output_path('pruned_variants.ht')
    pruned_variant_table.write(pruned_variant_table_path)


if __name__ == '__main__':
    main()
