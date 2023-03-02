# Create sites table for HGDP + 1kG datasets

This runs a Hail query script in Dataproc using Hail Batch in order to create a sites table for an input dataset and the combined HGDP + 1kG datasets. To run, use pip to install the analysis-runner, then execute the following command:

```sh
analysis-runner --dataset {input-dataset} \
--access-level {access-level} --output-dir "vds/combiner" \
--description "create sites table" python3 main.py
```
