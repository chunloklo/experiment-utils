import os
import sys
sys.path.append(os.getcwd())

import generate_configs
# from ..generate_configs import get_sorted_configuration_list_from_dict

# get_configuration_list function is required for 
def get_configuration_list():
    parameter_dict = {
        "seed": list(range(0, 100)),
        "param0": [100, 0, 55]
    }

    config_list = generate_configs.get_sorted_configuration_list_from_dict(parameter_dict)
    return config_list