from mpi4py import MPI
from mpi4py.util import dtlib
import numpy as np
import sys
import mpi4py

mpi4py.rc.thread_level = 'single' # or perhaps 'serialized'
mpi4py.rc.threads = False

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

datatype = MPI.UINT64_T
np_dtype = dtlib.to_numpy_dtype(datatype)
itemsize = datatype.Get_size()

N = 1
win_size = N * itemsize if rank == 0 else 1
win = MPI.Win.Allocate(win_size, comm=comm)

buf = np.empty(N, dtype=np_dtype)
if rank == 0:
    buf.fill(100)
    win.Lock(rank=0)
    win.Put(buf, target_rank=0)
    win.Unlock(rank=0)
comm.Barrier()


win.Lock(rank=0)
win.Get(buf, target_rank=0)

size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()
sys.stdout.write(
    "Hello, World! I am process %d of %d on %s. Queue is currently at %d\n"
    % (rank, size, name, buf))

buf += 1
win.Put(buf , target_rank=0)
win.Unlock(rank=0)