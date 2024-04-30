#!/bin/bash
#SBATCH --job-name=parquet_creator_%j
#SBATCH -D .
#SBATCH --output=./logs/parquet_creator_stdout_%j.out
#SBATCH --error=./logs/parquet_creator_stderr_%j.err
#SBATCH --time=00-02:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=64
#SBATCH --ntasks=1

 
source ../../../cluster_config/activate_env_amd.sh


echo COMMAND: python3 parquet_creator.py 
python3 parquet_creator.py 
