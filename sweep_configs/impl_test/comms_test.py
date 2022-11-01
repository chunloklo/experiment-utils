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

if rank == 0:
    for i in range(1, size):
        data = comm.recv(source=i, tag=i)
        print(data)
else:
    data = {'rank': rank}
    comm.send(data, dest=0, tag=rank)


