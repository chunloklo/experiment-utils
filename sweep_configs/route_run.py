from common import get_run_function_from_file_path, run_with_optional_aux_config

def run(config: dict, aux_config=None):
    # Assumes that the config has a key called 'run_path'
    assert 'run_path' in config, "A 'run_path' key is required in config for it to route to the correct run function."
    run_path = config['run_path']

    # Getting run function from run_path
    run_func = get_run_function_from_file_path(run_path)

    run_with_optional_aux_config(run_func, config, aux_config)
    return