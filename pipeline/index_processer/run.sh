#!/bin/bash
#SBATCH --job-name=p2txt_organised
#SBATCH -D .
#SBATCH --output=./stdout-%j.out
#SBATCH --error=./stderr-%j.err
#SBATCH --time=2-00:00:00
#SBATCH --cpus-per-task=8
#SBATCH --ntasks=20

# Clusters setup
if uname -a | grep -q amd
then
    module load impi intel greasy
else
    module load greasy
fi
export I_MPI_FABRICS=ofi
# Fix for PMI errors on CTE-AMD
export I_MPI_PMI_VALUE_LENGTH_MAX=512



export GREASY_LOGFILE=greasy.log
export GREASY_RESTARTFILE=greasy_restartfile_step.rst



# Run greasy
greasy tasks.greasy  
