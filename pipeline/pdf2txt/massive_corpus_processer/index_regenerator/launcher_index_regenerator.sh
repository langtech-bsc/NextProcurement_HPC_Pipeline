#!/bin/bash
#SBATCH --job-name=np_launcher_regen_%j
#SBATCH -D .
#SBATCH --output=./logs/np_launcher_regen_%j.out
#SBATCH --error=./logs/np_launcher_regen_%j.err
#SBATCH --time=00-02:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=1

#MN4
module load python/3.7.4                                                


python3 index_regenerator.py