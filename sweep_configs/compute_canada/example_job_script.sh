#!/bin/sh
#SBATCH --account=rrg-whitem
#SBATCH --time=00:5:00
#SBATCH --ntasks=33
#SBATCH --mem-per-cpu=4000M
#SBATCH --mail-user=chunlok@ualberta.ca
#SBATCH --mail-type=ALL
source ~/env-38/bin/activate
cd /project/6010404/chunlok/dyna-options
mpiexec python sweep_configs/run_mpi.py src/run_experiment.py experiments/chunlok/mpi/dyna_sweep.py 