# Generate combined VDS for HGDP + 1kG datasets

This runs a Hail query script in Dataproc using Hail Batch in order to create a combined VDS file for the hgdp-1kG + PROPHECY datasets. To run, use pip to install the analysis-runner, then execute the following command:

```sh
analysis-runner --dataset prophecy \
--access-level test --output-dir "vds/combiner" \
--description "combine HGDP/1kG/PROPHECY" python3 main.py
```
