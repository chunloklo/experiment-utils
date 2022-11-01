from typing import Tuple
import numpy as np

def perf_average(data: np.array, percentage: Tuple[float, float]=(0, 1), axis=1):
    assert percentage[0] >= 0 and percentage[1] <= 1, f'Percentage average must be between 0 and 1. Instead, the function got {percentage}'
    
    # Short cutting slicing percentage if you're getting the entire data anyways
    if percentage[0] == 0 and percentage[1] == 1:
        return np.mean(data)

    start_index = int(percentage[0] * data.shape[axis])
    end_index = int(percentage[1] * data.shape[axis])

    sliced_data = np.take(data, range(start_index, end_index), axis=axis)

    return np.mean(sliced_data)