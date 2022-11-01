from itertools import product
import code
from typing import Dict
from copy import deepcopy

# Getting a sorted configuration list. This is mostly to make sure that index is consistent when calling run_single.py
# It shouldn't really be a bottleneck when running experiments, but possible if the configuration list is absolutely massive.
def get_sorted_configuration_list_from_dict(configuration_collection_dict: Dict[str, list]):
    """Returns a sorted list of configurations from a dictionary of parameters.

    Args:
        configuration_collection_dict (Dict[str, list]): The dictionary of parameters from which the configuration list will be generated.

    Returns:
        List[Dict]: A list of configurations in the form of indiviudal dictionaries
    """
    collection = {}
    lengths = {}

    # sort dict keys, values, and populate lengths
    for k in sorted(configuration_collection_dict.keys()):
        k_list = configuration_collection_dict[k]
        assert isinstance(k_list, list), f'Parameter dict needs to have each value be a list. Key {k} is not a list'

        assert len(set(k_list)) == len(k_list), f'There are duplicates in the param list for {k}, list: {sorted(k_list)}'

        collection[k] = sorted(k_list)
        lengths[k] = len(k_list)
    
    coordinates = product(*[range(x) for x in lengths.values()])
    keys = list(collection.keys())

    def get_param_from_coord(coord):
        param = {}
        for key_index, i in enumerate(coord):
            key = keys[key_index]
            # Copying here to somewhat ensure that this is a deep copy.
            param[key] = deepcopy(collection[key][i])
        
        return param

    return [get_param_from_coord(coord) for coord in coordinates]

# Some debug code to see what this does
if __name__ == "__main__":
    param_dict = {
        'env': ['GrazingWorldAdam'],
        'alpha': [0.1, 0.2, 0.3, 0.5],
        'agent': ['DynaOptions_Tab'],
        'kappa': [0.9, 0.4, 0.2],
        'gamma': [0.99, 0.95, 0.9],
    }

    param_list = get_sorted_configuration_list_from_dict(param_dict)
    print(len(param_list))

    code.interact(local=locals())