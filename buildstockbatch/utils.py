import enum
import inspect
import os
import logging
import traceback
import yaml
import pandas as pd


logger = logging.getLogger(__name__)


class ContainerRuntime(enum.Enum):
    DOCKER = 1
    SINGULARITY = 2
    LOCAL_OPENSTUDIO = 3


def read_csv(csv_file_path, **kwargs) -> pd.DataFrame:
    default_na_values = pd._libs.parsers.STR_NA_VALUES
    df = pd.read_csv(csv_file_path, na_values=list(default_na_values - {"None", "NA"}), keep_default_na=False, **kwargs)
    return df


def path_rel_to_file(startfile, x):
    if os.path.isabs(x):
        return os.path.abspath(x)
    else:
        return os.path.abspath(os.path.join(os.path.dirname(startfile), x))


def get_project_configuration(project_file):
    try:
        with open(project_file) as f:
            cfg = yaml.load(f, Loader=yaml.SafeLoader)
    except FileNotFoundError as err:
        logger.error('Failed to load input yaml for validation')
        raise err

    # Set absolute paths
    cfg['buildstock_directory'] = path_rel_to_file(project_file, cfg['buildstock_directory'])
    # if 'precomputed_sample' in cfg.get('baseline', {}):
    #     cfg['baseline']['precomputed_sample'] = \
    #         path_rel_to_file(project_file, cfg['baseline']['precomputed_sample'])
    if 'weather_files_path' in cfg:
        cfg['weather_files_path'] = path_rel_to_file(project_file, cfg['weather_files_path'])

    return cfg


def _str_repr(obj, list_max=20, dict_max=20, string_max=100):
    if type(obj) is str:
        if len(obj) <= string_max:
            return f"'{obj}'"
        else:
            return f"'{obj[0:string_max//2]}...{len(obj)}...{obj[-string_max//2:]}'"
    elif type(obj) in [int, float]:
        return _str_repr(str(obj), list_max, dict_max, string_max)
    elif type(obj) is list:
        txt = "[" + ",".join([_str_repr(item, list_max, dict_max, string_max) for item in obj[0:list_max]])
        if len(obj) > list_max:
            txt += f" ...{len(obj)}"
        txt += "]"
        return txt
    elif type(obj) is tuple:
        txt = "(" + ",".join([_str_repr(item, list_max, dict_max, string_max) for item in obj[0:list_max]])
        if len(obj) > list_max:
            txt += f" ...{len(obj)}"
        txt += ")"
        return txt
    elif type(obj) is set:
        obj = list(obj)
        txt = "{" + ",".join([_str_repr(item, list_max, dict_max, string_max) for item in obj[0:dict_max]])
        if len(obj) > dict_max:
            txt += f" ...{len(obj)}"
        txt += "}"
        return txt
    elif type(obj) is dict:
        keys = list(obj.keys())
        txt = "{" + ",".join([f"{_str_repr(key, list_max, dict_max, string_max)}:"
                              f" {_str_repr(obj[key], list_max, dict_max, string_max)}" for key in keys[0:dict_max]])
        if len(keys) > dict_max:
            txt += f" ...{len(keys)}"
        txt += "}"
        return txt
    else:
        return str(obj)


def get_error_details():
    text = ""
    text += traceback.format_exc()
    frames = inspect.trace()
    for frame in frames:
        text += f'\nIn file: {frame[1]}, module {str(frame[3])} line: {frame[2]} \n'
        text += "Local Variables: "
        for var, value in frame[0].f_locals.items():
            text += _str_repr(var) + ":" + _str_repr(value)
            text += "\n"
    return text


def log_error_details(output_file="buildstockbatch_crash_details.log"):
    def log_error_decorator(func):
        def run_with_error_capture(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                with open(output_file, "a") as f:
                    text = "\n" + "#" * 20 + "\n"
                    text += get_error_details()
                    f.write(text)
                raise
        return run_with_error_capture

    return log_error_decorator
