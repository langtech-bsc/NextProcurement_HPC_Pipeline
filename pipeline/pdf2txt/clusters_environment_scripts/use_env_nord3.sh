module load python/3.7.12                                                  
unset PYTHONPATH
source <INTERNAL GPFS PATH>NextProcurement_v2/virtual_envs/env_nord3/bin/activate
export LD_LIBRARY_PATH=<INTERNAL GPFS PATH>bne/eval_amd/scripts_to_run/external-lib:$LD_LIBRARY_PATH
module load intel impi mkl greasy poppler gcc leptonica tesseract

export TESSDATA_PREFIX=/gpfs/apps/AMD/TESSERACT/4.1.1/GCC/share/tessdata/


# Tesseact performance fix
export OMP_THREAD_LIMIT=1