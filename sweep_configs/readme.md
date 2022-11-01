# sweep_configs
This is a small library for running many independent experiments to sweep over different configurations. It's designed to do just that, and nothing more.

## Requirements
This library requires MPI, `mpi4py` and `numpy` to be installed.

## Overview
This library's run files takes in paths to multiple python files in order to make this happen. For each collection of experiments that you want to run, you need the following 2 things:

1. A python file that contains a `get_configuration_list` function that returns a list of configurations (in the form of a list) that you want to run.
2. A python file that contains a `run(config)` function that runs your experiment with a specific configuration.

`run_mpi.py` is designed to be ran with MPI and will run all configurations returned from `get_configuration_list` in parallel in the number of nodes available.

Example command:
```mpiexec -n 8 python run_mpi.py <run_function_file> <configuration_list_file> ```

`run_single.py` is designed to be a debugging file that allows you to run a single configuration from the configuration list before using `run_mpi.py`. It allows you to specify exactly which index you want to run from your configuration list.

Example command:
```python run_single.py <run_function_file> <configuration_list_file> <index>```

## Including run path in config
One thing you might want to do is include the path of the run function file in the config so that you don't forget which file was used to run which config. `route_run.py` supports this if you allow the config to be accessed with the `run_path` key, i.e. `run_path = config['run_path']`. This is simple if your config is a dictionary: simply set `config['run_path'] = '<run_function_file>'`

Once that's done, you can use `route_run.py` as the run function file when running the commands above. `route_run.py`'s run function will then execute the run function in `config['run_path']`.

Example command:
```mpiexec -n 8 python run_mpi.py route_run.py <configuration_list_file> ```

## Auxiliary configurations
On occasions, especially with your debug experiments, you might want to run your experiment with a set of auxiliary parameters that doesn't affect the experiment results. For example, you might want to show debug information, progress, or print-outs on local runs of your experiment, or you might want debug information from your sweep experiments. Auxiliary configurations allows you to provide a set of additional parameters to your experiment for these cases. 

**The intention for the auxiliary configurations is for configs that will NOT affect the experiment results.** But if course you can do what you want, just my advice.

To add an auxiliary configuration, create a python file with a function named `get_auxiliary_configuration`, and provide the path to the file when running as the third positional argument.

Example `python run_single.py <run_function_file> <configuration_list_file> <aux_config_file> <index>`

Then, you need to modify `run` in your run file such that it accepts arguments in the form of `run(config, aux_config=<Default>)`.

## Example
To see how to implement your own sweep, check out files under `sweep_configs/example/`. Run `dummy_bash.sh` to see an example output.

## Performing grid search
A common way to perform sweeps is through grid-search. `generate_configs.get_sorted_configuration_list_from_dict` is designed to generate a configuration list just for that scenario. It generates a sorted list of configurations from a dictionary describing what parameter values you want to sweep over.

## Running on Compute Canada
Example scripts on how to use this library to run configuration sweeps can be found in `sweep_configs/compute_canada`.

