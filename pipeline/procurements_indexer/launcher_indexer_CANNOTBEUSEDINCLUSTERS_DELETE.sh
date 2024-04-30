#!/bin/bash
#SBATCH --job-name=np_indexer
#SBATCH -D .
#SBATCH --output=./logs/index_utes_docs_stdout_%j.out
#SBATCH --error=./logs/index_utes_docs_stderr_%j.err
#SBATCH --time=00-02:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --ntasks=1

 
 


#module load intel/2018.4 mkl/2018.4 python/3.7.4
#source env/bin/activate

source ../cluster_config/activate_env_amd.sh


#python3 indexer_utes_docs.py -i UTES_INFO_FILES/utes.parquet -o INDEX_PROCUREMENTS --list_of_pdfnames_file INDEX_UNPROCESSED_FILES/gpfs_index.txt
echo COMMAND: python3 index_utes_docs.py -i INDEXES/UTES_INFO_FILES/utes.parquet -o INDEXES/TAGGED_PROCUREMENTS --list_of_pdfnames_file INDEXES/ORIGINAL_PDF_LIST/cleaned_index.txt
python3 index_utes_docs.py -i INDEXES/UTES_INFO_FILES/utes.parquet -o INDEXES/TAGGED_PROCUREMENTS --list_of_pdfnames_file INDEXES/ORIGINAL_PDF_LIST/cleaned_index.txt
