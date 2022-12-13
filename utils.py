import pathlib
import glob
import sys
import yaml
import pandas as pd

def list_config_files(data_repo_location):
    config_directory = pathlib.Path(data_repo_location, "config_files")
    if config_directory.exists():
        config_files = config_directory.glob('*.yml')
        return config_files
    else:
        print("\nNot found: config_files directory\n")
        sys.exit(1)

def load_pipeline(config_file):
    profile_config = {}
    with open(config_file, "r") as stream:
        for data in yaml.load_all(stream, Loader=yaml.FullLoader):
            if "pipeline" in data.keys():
                pipeline = data
            else:
                process = data["process"]
                if not process:
                    continue
                batch = data["batch"]
                plates = [str(x["name"]) for x in data["plates"] if x["process"]]
                profile_config[batch] = plates

    return pipeline, profile_config

def process_steps(pipeline):
    parameters = pd.DataFrame()
    if "aggregate" in pipeline and pipeline["aggregate"]["perform"]:
        parameters = pd.concat([parameters, pd.DataFrame({"step": "aggregate", "suffix": "", "level": ""}, index=[0])])
    if "annotate" in pipeline and pipeline["annotate"]["perform"]:
        parameters = pd.concat([parameters, pd.DataFrame({"step": "annotate", "suffix": "", "level": ""}, index=[0])])
    if "normalize" in pipeline and pipeline["normalize"]["perform"]:
        parameters = pd.concat([parameters, pd.DataFrame({"step": "normalize", "suffix": "", "level": ""}, index=[0])])
    if "normalize_negcon" in pipeline and pipeline["normalize_negcon"]["perform"]:
        parameters = pd.concat([parameters, pd.DataFrame({"step": "normalize", "suffix": "_negcon", "level": ""}, index=[0])])
    if "feature_select" in pipeline and pipeline["feature_select"]["perform"]:
        if pipeline["feature_select"]["gct"]:
            parameters = pd.concat([parameters, pd.DataFrame({"step": "feature_select_gct", "suffix": "", "level": f'_{pipeline["feature_select"]["level"]}'}, index=[0])])
        else:
            parameters = pd.concat([parameters, pd.DataFrame({"step": "feature_select", "suffix": "", "level": f'_{pipeline["feature_select"]["level"]}'}, index=[0])])
    if "feature_select_negcon" in pipeline and pipeline["feature_select_negcon"]["perform"]:
        if pipeline["feature_select_negcon"]["gct"]:
            parameters = pd.concat([parameters, pd.DataFrame({"step": "feature_select_gct", "suffix": "_negcon", "level": f'_{pipeline["feature_select"]["level"]}'}, index=[0])])
        else:
            parameters = pd.concat([parameters, pd.DataFrame({"step": "feature_select", "suffix": "_negcon", "level": f'_{pipeline["feature_select"]["level"]}'}, index=[0])])
    if "quality_control" in pipeline and pipeline["quality_control"]["perform"]:
        if pipeline["quality_control"]["summary"]["perform"]:
            parameters = pd.concat([parameters, pd.DataFrame({"step": "qc_summary", "suffix": "", "level": ""}, index=[0])])
        if pipeline["quality_control"]["heatmap"]["perform"]:
            parameters = pd.concat([parameters, pd.DataFrame({"step": "qc_heatmap", "suffix": "", "level": ""}, index=[0])])
    
    return parameters.reset_index(drop=True)

def generate_filepath(data_repo_location, batch, plate, parameters):
    step = parameters["step"]
    suffix = parameters["suffix"]
    level = parameters["level"]
    filepath = []

    if step == "aggregate":
        filepath.append(pathlib.Path(data_repo_location, "profiles", batch, plate, f'{plate}.csv.gz'))
    elif step == "annotate":
        filepath.append(pathlib.Path(data_repo_location, "profiles", batch, plate, f'{plate}_augmented.csv.gz'))
    elif step == "normalize":
        filepath.append(pathlib.Path(data_repo_location, "profiles", batch, plate, f'{plate}_normalized{suffix}.csv.gz'))
    elif step == "feature_select":
        filepath.append(pathlib.Path(data_repo_location, "profiles", batch, plate, f'{plate}_normalized_feature_select{suffix}{level}.csv.gz'))
    elif step == "feature_select_gct":
        filepath.append(pathlib.Path(data_repo_location, "gct", batch, f'{plate}_normalized_feature_select{suffix}{level}.csv.gz'))
    elif step == "qc_summary":
        filepath.append(pathlib.Path(data_repo_location, "quality_control", "summary", 'summary.tsv'))
    elif step == "qc_heatmap":
        filepath.append(pathlib.Path(data_repo_location, "quality_control", "heatmap", batch, plate, f'{plate}_cell_count.png'))
        filepath.append(pathlib.Path(data_repo_location, "quality_control", "heatmap", batch, plate, f'{plate}_correlation.png'))
        filepath.append(pathlib.Path(data_repo_location, "quality_control", "heatmap", batch, plate, f'{plate}_position_effect.png'))
    
    return filepath