#!/bin/bash
#SBATCH --job-name=np_ll_processer
#SBATCH -D .
#SBATCH --output=./slurm_logs/ll_processer_stdout_%j.out
#SBATCH --error=./slurm_logs/ll_processer_stderr_%j.err
#SBATCH --time=2-00:00:00
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=1

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




source ../../cluster_config/activate_env_amd.sh
python3 ll_processer.py