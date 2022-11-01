# Lots of analysis results would be nice to be cached. Here, we provide a generic method for caching objects.

from typing import Dict
import os
import pickle as pkl

# Simple wrapper for loading and saving pkl objects
def load_pkl(filename):
    with open(filename, 'rb') as fil:
        data = pkl.load(fil)
    return data

def save_pkl(obj, filename):
    if len(os.path.dirname(filename)) > 0:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'wb') as fil:
        pkl.dump(obj, fil)


def cache_local_file(cache_file_name: str, key: str, get_cached=False):
    def cache_local_file_decorator(func):
        def cache_local_file_wrapper(*args, **kwargs):
            if get_cached and os.path.isfile(cache_file_name):
                # Return from cache if exists and not refreshing
                    cache = load_pkl(cache_file_name)
                    if key in cache:
                        print(f'Loading cached data from key {key} and file {cache_file_name}')
                        return cache[key]
                    
            try:
                cache_data = load_pkl(cache_file_name)
            except FileNotFoundError as e:
                cache_data = {}
            func_data = func(*args, **kwargs)

            cache_data[key] = func_data
            save_pkl(cache_data, cache_file_name)
            return func_data

        return cache_local_file_wrapper
    return cache_local_file_decorator