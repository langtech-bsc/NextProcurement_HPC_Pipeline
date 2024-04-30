module load python/3.7.4                                                
unset PYTHONPATH
source <INTERNAL GPFS PATH>NextProcurement_v2/virtual_envs/env_mn/bin/activate
export LD_LIBRARY_PATH=<INTERNAL GPFS PATH>bne/eval_amd/scripts_to_run/external-lib:$LD_LIBRARY_PATH
#module load gcc
module load gcc/8.1.0
module load leptonica
module load TESSERACT/20-03-2020
module load greasy

export TESSDATA_PREFIX=/gpfs/apps/AMD/TESSERACT/4.1.1/GCC/share/tessdata/



# Tesseact performance fix
export OMP_THREAD_LIMIT=1
