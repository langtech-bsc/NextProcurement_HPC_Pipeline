#!/bin/bash
#SBATCH --job-name=pdf2txt_massive_scripts_creator_%j
#SBATCH -D .
#SBATCH --output=./logs/pdf2txt_massive_scripts_creator_%j.out
#SBATCH --error=./logs/pdf2txt_massive_scripts_creator_%j.err
#SBATCH --time=00-02:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=1

 
#source ../../../cluster_config/activate_env_amd.sh



n_docs_per_greasy_line=100



#CPU CONFIG FOR AMD
echo COMMAND: pdf2txt_massive_scripts_creator.py -n 10000 --cpus_per_task 2 --ntasks 32 --n_docs_per_greasy_line $n_docs_per_greasy_line  --suffix amd
python3 pdf2txt_massive_scripts_creator.py -n 10000 --cpus_per_task 2 --ntasks 32 --n_docs_per_greasy_line $n_docs_per_greasy_line  --suffix amd
#--ntasks 16


#CPU CONFIG FOR NORD3
echo COMMAND: pdf2txt_massive_scripts_creator.py -n 10000 --cpus_per_task 4 --ntasks 16 --n_docs_per_greasy_line $n_docs_per_greasy_line --suffix nord3
python3 pdf2txt_massive_scripts_creator.py -n 10000 --cpus_per_task 4 --ntasks 16 --n_docs_per_greasy_line $n_docs_per_greasy_line --suffix nord3
--ntasks 8


#CPU CONFIG FOR MN
echo COMMAND: pdf2txt_massive_scripts_creator.py -n 10000 --cpus_per_task 1 --ntasks 32 --n_docs_per_greasy_line $n_docs_per_greasy_line --suffix mn
python3 pdf2txt_massive_scripts_creator.py -n 10000 --cpus_per_task 1 --ntasks 32 --n_docs_per_greasy_line $n_docs_per_greasy_line --suffix mn
# --ntasks 16