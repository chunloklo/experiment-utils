# Run this file to pack your database if it grows too large
from experiment_utils.data_io.io.zodb_io import pack_db
import argparse

parser = argparse.ArgumentParser(description='MPI file that is ran on each task that is spawned through mpiexec or similar functions')
parser.add_argument('db_folder', help='The folder the database is in')
args = parser.parse_args()

pack_db(args.db_folder)