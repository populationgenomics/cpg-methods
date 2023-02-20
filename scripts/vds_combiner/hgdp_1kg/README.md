# Generate combined VDS for HGDP + 1kG datasets

This runs a Hail query script in Dataproc using Hail Batch in order to create a combined VDS file for the HGDP + 1kG datasets. To run, use conda to install the analysis-runner, then execute the following command:

```sh
analysis-runner --dataset cpg-methods \
--access-level test --output-dir "vds/hgdp_1kg/v0" \
--description "combine HGDP/1kG" python3 main.py
```
