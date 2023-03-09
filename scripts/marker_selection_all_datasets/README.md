# Create sites table for input dataset + HGDP/1kG combined dataset

This runs a Hail query script in Dataproc using Hail Batch in order to create a representative subset of variants from an input dataset and the combined HGDP + 1kG datasets. Variants are chosen based off of the following criteria: are biallelic, autosomal, have an allele frequency above 1%, have a call rate above 99%, and have an inbreeding coefficient > -0.25.

To run, use pip to install the analysis-runner, then execute the following command:

```sh
analysis-runner --dataset $INPUT_DATASET \
--access-level $ACCESS_LEVEL --output-dir "vds/combiner" \
--description "create sites table" python3 main.py \
--path $PATH --output-version $OUTPUT_VERSION
```
