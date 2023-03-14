# Create sites table for HGDP + 1kG datasets

This runs a Hail query script in Dataproc using Hail Batch in order to create a sites table for the combined HGDP + 1kG datasets. To run, use pip to install the analysis-runner, then execute the following command:

```sh
analysis-runner --dataset hgdp-1kg \
--access-level test --output-dir "sites_table/v1-0" \
--description "create HGDP/1kG sites table" python3 main.py
```
