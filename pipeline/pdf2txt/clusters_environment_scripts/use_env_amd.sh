module load gcc/10.2.0 rocm/5.1.1 intel/2018.4 mkl/2018.4 python/3.7.4 
unset PYTHONPATH
source <INTERNAL GPFS PATH>NextProcurement_v2/virtual_envs/env_amd/bin/activate
export LD_LIBRARY_PATH=<INTERNAL GPFS PATH>bne/eval_amd/scripts_to_run/external-lib:$LD_LIBRARY_PATH
module load leptonica tesseract
module load impi intel greasy


# Tesseact performance fix
export OMP_THREAD_LIMIT=1


export I_MPI_FABRICS=ofi
# Fix for PMI errors on CTE-AMD
export I_MPI_PMI_VALUE_LENGTH_MAX=512
