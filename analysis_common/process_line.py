import numpy as np 

def get_mean_std(data: np.array):
    """
    Returns the mean and standard deviation from the data.

    Args:
        data (np.array): [The data is assumed to be NxM where N is the number of runs, and M is the number of steps]

    Returns:
        [(np.array, np.array)]: [Returns the (mean, std) of the data]
    """
    assert len(data.shape) == 2, f'data with more than 2 dimension is not supported. Input data has {len(data.shape)} dimensions'
    # Assumes the first dimension is the number of runs, the second is the number of log steps
    data_mean = np.mean(data, axis=0)
    data_std = np.std(data, axis=0)
    return data_mean, data_std

def get_mean_stderr(data: np.array):
    data_mean, data_std = get_mean_std(data)
    data_stderr = data_std / np.sqrt(data.shape[0])
    return data_mean, data_stderr


def mean_chunk_data(data: np.array, chunk_size: int, axis=0) -> np.array:
    """Chunks the data along some axis and averages the data along that chunk, rejoining them afterwards.
    This is equivalent to chunking along some axis and getting the mean of that chunk.

    Args:
        data (np.array): The data to be chunked
        chunk_size (int): The size of each chunk
        axis (int, optional): Axis by which to chunk data. Defaults to 0.

    Returns:
        [type]: [description]
    """
    iter_range = range(0, data.shape[axis], chunk_size)

    def get_mean_chunk(start_index):
        end_index = min(data.shape[axis], start_index + chunk_size)
        chunk = np.take(data, range(start_index, end_index), axis=axis)
        mean_chunk = np.mean(chunk, axis=axis, keepdims=True)
        return mean_chunk

    data = np.concatenate([get_mean_chunk(start_index) for start_index in iter_range], axis=axis)
    return data