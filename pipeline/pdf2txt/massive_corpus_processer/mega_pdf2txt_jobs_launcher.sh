#!/bin/bash


# DESCRIPTION: Meta launcher of jobs
# Given an start-end index, will launch several pdf2txt jobs
#       Interactive tool

#USAGE
#    bash mega_pdf2txt_jobs_launcher.sh



# MACROS
n_docs_per_script=10000





echo "Enter the desired environment (amd, nord3 or mn) (you must be running this on that cluster)"
read -p 'ENV:' venv
printf "\n\n"


#created_on_2024_04_16_

case $venv in
        "amd")   
            echo Selected env: $venv
            rootfolder=<INTERNAL BSC PATH>/NextProcurement_dev/pipeline/pdf2txt/massive_corpus_processer/subscripts/created_on_2024_04_16_*_amd

        ;;

        "nord3" )   
            echo Selected env: $venv
            rootfolder=<INTERNAL BSC PATH>/NextProcurement_dev/pipeline/pdf2txt/massive_corpus_processer/subscripts/created_on_2024_04_16_*_nord3

        ;;

        "mn" )   
            echo Selected env: $venv
            rootfolder=<INTERNAL BSC PATH>/NextProcurement_dev/pipeline/pdf2txt/massive_corpus_processer/subscripts/created_on_2024_04_16_*_mn


        ;;

        *)
            printf 'ERROR: Invalid enviroment choosed! Options are:\n'
            printf '\t amd \n\t nord3 \n\t mn\n'
            exit 1
        ;;
esac



echo Root folder will be: ${rootfolder}
printf "\n\n\n\n"



echo '----------------------------------------------------------'
echo "----> Enter the starting index (count starts at 0)..."
read -p 'Initial index:' init
printf "\n"
echo '----------------------------------------------------------'
echo "----> Now the index to be taken as the last one (INCLUDED)..."
read -p 'Last Index:' end
printf "\n\n"
echo Starting launcher from index: $init to: $end 



for ((i=init ; i<=end ; i++)); do
    echo $i
    #echo cd ${rootfolder}_$venv/launcher_n_${i}_to_process_*
    #echo launcher_n_${i}_to_process_*.sh
    #printf "\n"
    echo Launching launcher_n_${i}_to_process_${n_docs_per_script}_docs.sh ...
    cd ${rootfolder}/launcher_n_${i}_to_process_${n_docs_per_script}_docs
    sbatch  launcher_n_${i}_to_process_${n_docs_per_script}.sh $venv
    printf "\n"
done;