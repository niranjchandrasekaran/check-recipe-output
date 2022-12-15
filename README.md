# check-recipe-output
Check if all files were created by comparing the created files with profiling-recipe's config.yml files.

## Downloading the script
Clone the repo outside the data repo

```bash
git clone git@github.com:niranjchandrasekaran/check-recipe-output.git
```

## Installing the environment

Download and install [miniconda](https://docs.conda.io/en/latest/miniconda.html) and then install the required packages

```bash
conda env create --force --file environment.yml
conda activate check-recipe-output
```

## Running the script

The code requires two inputs
1. Location of the data repository
2. Location where the output file should be written to


```bash
python check-recipe-output.py --location <LOCATION OF THE DATA REPO> --output <LOCATION OF THE OUTPUT FOLDER>
```

The script creates a folder called `output` and writes the names of missing and empty files to `output/missing_or_emtpy_files.csv`. If there are no missing or empty files, the code exits after printing the following message.

```bash
All files are present. Output file was not created.
```
