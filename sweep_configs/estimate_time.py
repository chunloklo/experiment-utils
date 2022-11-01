import argparse
from common import get_configuration_list_from_file_path
from datetime import timedelta
# Parsing arguments
parser = argparse.ArgumentParser(description='MPI file that is ran on each task that is spawned through mpiexec or similar functions')
parser.add_argument('config_path', nargs='?', help='Path to the Python parameter file that contains a get_configuration_list function that returns a list of parameters to run')
args = parser.parse_args()

configuration_path = args.config_path

# Getting configuration list from parameter_path
configuration_list = get_configuration_list_from_file_path(configuration_path)
num_configs = len(configuration_list)

print(f"Estimating time required to run {num_configs} configurations")

while True:
    try:
        time_per_job = input("Please enter the amount of time needed per configuration. <D:H:M, H:M, M>: ")
        time_args = time_per_job.split(":")
        time_args = [int(t) for t in time_args]
        num_split = len(time_args)
        
        if num_split < 3:
            time_args = [0] * (3 - num_split) + time_args

        time_per_job = timedelta(days = time_args[0], hours = time_args[1], minutes = time_args[2])

        print(f"Inputted time: {time_per_job.days} days, {time_per_job.seconds//3600} hours, {(time_per_job.seconds//60)%60} minutes")
    except ValueError as e:
        print(f'Sorry, invalid input. Error: {str(e)}')
    else:
        break

while True:
    try:
        ntasks = int(input("Please enter the number of tasks you wish to have: "))
        if (ntasks <= 0):
            raise ValueError('Bad integer')
    except ValueError as e:
        print(f'Sorry, please enter a valid number of tasks {str(e)}')
    else:
        break

seconds = time_per_job.seconds * num_configs / ntasks
time_required = timedelta(seconds = seconds)
print(f'Estimated time per CPU needed is -t={time_required.days}:{time_required.seconds//3600}:{(time_required.seconds//60)%60}')
exit()
