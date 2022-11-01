# [chunlok-2022-01-11] Common functions that doesn't have anywhere else to go 
# If this file gets too big, then it's probably time to separate things out.

import importlib.util
import argparse
from typing import Any, Callable, Optional
import traceback

def get_configuration_list_from_file_path(file_path: str):
    # [chunlok-2022-01-11] This little duplicate code here is likely okay for now (similar code in get_run_function_from_file_path) 
    # Getting parameter list from parameter_path
    param_spec = importlib.util.spec_from_file_location("ParamModule", file_path)
    ParamModule = importlib.util.module_from_spec(param_spec)
    param_spec.loader.exec_module(ParamModule)
    try:
        parameter_list = ParamModule.get_configuration_list()
    except AttributeError as e:
        print(f'Error occurred when obtaining configuration list from {file_path}.\nPlease ensure that it contains a get_configuration_list() function')
        raise e
    return parameter_list

def get_run_function_from_file_path(file_path: str):
    # [chunlok-2022-01-11] This little duplicate code here is likely okay for now (similar code in get_configuration_list_from_file_path) 
    # Getting run function from run_path
    run_spec = importlib.util.spec_from_file_location("RunModule", file_path)
    RunModule = importlib.util.module_from_spec(run_spec)
    run_spec.loader.exec_module(RunModule)
    try:
        run_func = RunModule.run
    except AttributeError as e:
        print(f'Error occurred when obtaining the run function from {file_path}.\nPlease ensure that it contains a run(Any) function')
        raise e
    return run_func

def get_aux_config_from_file_path(file_path: Optional[str]):
    if file_path is None:
        return None
    run_spec = importlib.util.spec_from_file_location("AuxConfigModule", file_path)
    AuxConfigModule = importlib.util.module_from_spec(run_spec)
    run_spec.loader.exec_module(AuxConfigModule)
    try:
        aux_config = AuxConfigModule.get_auxiliary_configuration()
    except AttributeError as e:
        print(f'Error occurred when obtaining the auxiliary configurations from {file_path}.\nPlease ensure that it contains a get_auxiliary_configuration() function')
        raise e
    return aux_config

def add_common_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Adds positional run_path, parameter_path, and auxiliary_config_path to the parser.

    Args:
        parser (argparse.ArgumentParser): ArgumentParser to add args to

    Returns:
        [argparse.ArgumentParser]: The parser with which the arguments have been added
    """
    parser.add_argument('run_path', help='Path to the Python run file that contains a run(parameter: dict) function that runs the experiment with the specified parameters')
    parser.add_argument('config_path', help='Path to the Python parameter file that contains a get_configuration_list function that returns a list of parameters to run')
    parser.add_argument('aux_config_path', nargs='?', help='Path to the Python auxiliary config file that contains a get_auxiliary_configuration function that a set of auxiliary')
    return parser

def run_with_optional_aux_config(run_func: Callable, config: Any, aux_config: Any):
    try:
        if aux_config is not None:
            run_func(config, aux_config)
        else:
            run_func(config)
    except Exception as exc:
        # Stops run errors from failing out the entire program if something occurs during the run
        print(traceback.format_exc())
        print(exc)