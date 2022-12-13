import argparse
import utils
import pandas as pd
import pathlib
import os

parser = argparse.ArgumentParser(description='Check the output of the profiling-recipe')
parser.add_argument("--location", help="Location of the data repository",  required=True)
parser.add_argument("--output", help="Output folder location",  required=True)
args = parser.parse_args()

data_location = args.location
output_location = args.output

config_file_paths = [_ for _ in utils.list_config_files(data_location)]

missing_files = pd.DataFrame()

for config_file_path in config_file_paths:
    config_file_name = config_file_path.name
    pipeline, pipeline_config = utils.load_pipeline(config_file_path)
    parameters = utils.process_steps(pipeline)
    for batch in pipeline_config:
        for plate in pipeline_config[batch]:
            for index, step_parameters in parameters.iterrows():
                filepath = utils.generate_filepath(data_location, batch, plate, step_parameters)

                if not filepath.exists() or os.path.getsize(filepath) == 0:
                    missing_files = pd.concat([missing_files, pd.DataFrame({'config_file': config_file_name, 'file': filepath}, index=[0])])

if missing_files.shape[0] == 0:
    print("All files are present. Output file was not created.")
else:
    if not pathlib.Path(output_location).exists():
        os.mkdir(output_location)
    missing_files[['config_file', 'file']].to_csv(f'{output_location}/missing_or_empty_files.csv', index=False)
