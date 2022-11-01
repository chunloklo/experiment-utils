# This is require to turn threading off before MPI is initialized
import mpi4py
mpi4py.rc.thread_level = 'single' # or perhaps 'serialized'
mpi4py.rc.threads = False

# This import statement also initializes MPI
from mpi4py import MPI

from mpi4py.util import dtlib
import numpy as np
import sys
import importlib.util
import argparse
import copy
import sys

# Standard MPI info
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = MPI.COMM_WORLD.Get_size()
name = MPI.Get_processor_name()

# Creating a counter for each task to grab tasks off of
# The counter will represent the next parameter that needs to be ran,
# Each task, when waiting, will grab the top available number and run it.
# This is repeated until the entire parameter list is ran

datatype = MPI.UINT64_T
np_dtype = dtlib.to_numpy_dtype(datatype)
itemsize = datatype.Get_size()

# Creating window
win_size = itemsize if rank == 0 else 1
win = MPI.Win.Allocate(win_size, comm=comm)

# Starting the counter at 0 
if rank == 0:
    # Creating buffer for each task
    buf = np.empty(1, dtype=np_dtype)
    buf.fill(0)
    win.Lock(rank=0)
    win.Put(buf, target_rank=0)
    win.Unlock(rank=0)
comm.Barrier()

accumulate = np.ones(1, dtype=np_dtype)
result = np.empty(1, dtype=np_dtype)

for _ in range(10):
    win.Lock(rank=0)
    win.Fetch_and_op(accumulate, result, target_rank=0)
    win.Unlock(rank=0)
    sys.stdout.write(
            f"Removed Lock! I am process {rank} of {size} on {name}. Result: {result}\n")

comm.Barrier()
win.Lock(rank=0)
win.Get(result, target_rank=0)
win.Unlock(rank=0)
sys.stdout.write(
        f"Removed Lock! I am process {rank} of {size} on {name}. Final Result: {result}\n")
        
sys.exit(0)



