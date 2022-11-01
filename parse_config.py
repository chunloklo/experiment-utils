from typing import Dict, Callable
from typing import Any, Optional

def get_and_validate_param(param_dict: Dict, key: str, valid_test: Optional[Callable[[Any], bool]] = None, default: Any = None, optional: bool = False):
    if not optional:
        if key not in param_dict:
            raise ValueError(f'No value exists for {key} but is not optional')
    param = param_dict.get(key, default)
    if valid_test is not None:
        check_valid(param, valid_test, f'Invalid {param} for key {key}')
    return param

def check_valid(value: Any, valid_test: Optional[Callable[[Any], bool]], message: Optional[str] = None):
    if not valid_test(value):
        if message is None:
            raise ValueError(f'{value} is invalid')
        else:
            raise ValueError(message)

    return value
    