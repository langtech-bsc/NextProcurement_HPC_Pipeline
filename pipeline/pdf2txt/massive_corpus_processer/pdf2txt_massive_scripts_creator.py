'''Given the list of pdf files under the directory
      <INTERNAL PATH>data/

    as a cleaned_index.txt,

    this script will create several pairs of:
        (launcher_script.sh + tasks.greasy)

    according to a splitting parameter, in order to scale and speed up the parallelization of
    processing the pdfs belongin to  NextProcurment corpus.

    
    Output will be by now written to an aux path, since the final path  of NextProc pipeline
    requires to have the procurements ordered by nptXXX id and metadated through the web api.


    
    * Things to keep in mind:
        - If a file has been correctly processed ( output path ending with'_processed_finished'):
            pdf2txt will automatically skip it, saving computation power! :D

        - Theorically, the greasy env is inherited by the current python env:
            so it will be handled as an argument on each launcher.sh <env> sub.script

        - This will not metadate docs, only process them with pdf2txt

'''

import argparse
import os
import sys
import logging
from datetime import datetime
import math



# MACROS
INPUT_PDF_LIST = '''<INTERNAL BSC PATH>/NextProcurement_dev/pipeline/INDEXES/ORIGINAL_PDF_LIST/cleaned_index.txt'''
INPUT_PDF_BASE_PATH = ''' <INTERNAL PATH>data/'''
OUTPUT_GPFS_PROCESSED_PDFS_PATHS = '''<INTERNAL GPFS PATH>NextProcurement/pipeline_executions/nextproc_17oct23_amd/data_processed'''
OUTPUT_FINAL_ORGANISED_GPFS_PROCESSED_PDFS_PATHS = '''<INTERNAL PATH>data_processed_organised'''


# PDF2TXT MACROS
PATH_TO_PDF2TXT = '''<INTERNAL BSC PATH>/NextProcurement_dev/pipeline/pdf2txt/only_txt_generator_version/pdf2txt'''
BASE_PATH_TO_PDF2TXT_ENV_SCRIPT = '''<INTERNAL GPFS PATH>NextProcurement_v2/scripts/venvs'''
'''                                 or (use_env_amd.sh)
                                    or (use_env_mn.sh)
                                    or (use_env_nord3.sh)

'''
PATH_TO_PDF2TXT_MODEL = '''<INTERNAL GPFS PATH>NextProcurement/model_trainer/output/NextProcurement/next_procurement_v0_8_0.00005_date_22-08-13_time_04-51-37'''


# SLURM for each SINGLE SCRIPTS MACROS
EXECUTION_PARENT_PATH = PATH_TO_PDF2TXT
#CPUS_PER_TASK = 4
#NTASKS = 128
#CPUS_PER_TASK = 1
#NTASKS = 8
CPUS_PER_TASK = 2
NTASKS = 16






def chunk_list_in_n_slices(l, n):
    ''' Primitive
        n number of elemetns per chunck
    '''
    for i in range(0, len(l), n):  
        yield l[i:i + n] 








def _write_launcher_sh_script(script_folder_path,script_name, id_subscript, args):
    '''Hardcoded (empirically tested) specifications for each script.
        Basically this prepares the greasy resouces and in slurm and calls greasy

        It will create a file to mark the entire execution has been completed
    '''


    script_content = f"""#!/bin/bash \n"""
    script_content += f"""#SBATCH --job-name=np_script_n_{id_subscript} \n"""
    #script_content += f"""#SBATCH -D {EXECUTION_PARENT_PATH} \n""" # will be controlled by greasy
    script_content += f"""#SBATCH -D . \n"""
    script_content += f"""#SBATCH --output=./logs/slurmlog_np_script_n_{id_subscript}_%j.out \n"""
    script_content += f"""#SBATCH --error=./logs/slurmlog_np_script_n_{id_subscript}_%j.err \n"""
    #script_content += f"""#SBATCH --ntasks={NTASKS} \n"""
    #script_content += f"""#SBATCH --cpus-per-task={int(CPUS_PER_TASK)} \n"""
    script_content += f"""#SBATCH --ntasks={int(args.ntasks)} \n"""
    script_content += f"""#SBATCH --cpus-per-task={int(args.cpus_per_task)} \n"""
    script_content += f"""#SBATCH --time=2-00:00:00 \n"""
    script_content += f"""\n\n\n\n"""

    script_content += f"""ENV=$1\n"""


    script_content += f"""#   USAGE:\n"""
    script_content += f"""#bash launcher_<*>.sh <amd|nord3|mn>\n\n\n"""





    script_content += f"""GREASY_LOGFILE=./logs/greasy_logfile.log \n"""
    script_content += """export GREASY_LOGFILE \n"""

    script_content += f"""GREASY_RESTARTFILE=./logs/greasy_restartfile.log \n"""
    script_content += """export GREASY_RESTARTFILE \n"""


    # (This should be alredy on each env activation script)
    # Module loading depending on the current cluster
    #script_content +=  """# Clusters setup \n"""
    #script_content += """if uname -a | grep -q amd \n"""
    #script_content += """then \n"""
    #script_content += """    module load impi intel greasy \n"""
    #script_content += """else \n"""
    #script_content += """    module load greasy \n"""
    #script_content += """fi \n"""
    #script_content += """export I_MPI_FABRICS=ofi \n"""
    #script_content += """# Fix for PMI errors on CTE-AMD \n"""
    #script_content += """export I_MPI_PMI_VALUE_LENGTH_MAX=512 \n"""



    # Python enviroment loading depending on the current cluster
    #script_content += f"""BASE_PATH_TO_PDF2TXT_ENV_SCRIPT={BASE_PATH_TO_PDF2TXT_ENV_SCRIPT} \n\n\n"""
    script_content += """case $ENV in \n"""
    script_content += """\t"amd"|"nord3"|"mn" ) \n"""
    script_content += f""" \t\tENV_PATH={BASE_PATH_TO_PDF2TXT_ENV_SCRIPT}/use_env_$ENV.sh\n"""
    script_content += """\t\t;; \n"""
    script_content += """\t*) \n"""
    script_content += """ \t\techo 'ERROR: Invalid enviroment choosed! Options are: amd, nord3 or mn'\n"""
    script_content += """ \t\texit 1\n"""
    script_content += """\t\t;; \n"""
    script_content += """esac \n"""
    script_content += """echo Activating environment located in $ENV_PATH ... \n"""
    script_content += """source $ENV_PATH \n"""



    script_content += """greasy tasks.greasy \n"""
    script_content += """touch logs/script_execution_finished.txt \n"""


    with open(os.path.join(script_folder_path, script_name),'w') as f:
        f.write(script_content)
        
        
        
        
        
def _get_line_greasy_for_doc(doc_name):
    '''To be called by _write_tasks_greasy_file func 
    
    '''

    # Theorically, the greasy env is inherited by the current python env
    #greasy_line = f'''[@ {PATH_TO_PDF2TXT} @] source {PATH_TO_PDF2TXT_ENV_SCRIPT}  && '''
    greasy_line = f'''[@ {PATH_TO_PDF2TXT} @] '''


    # With this line we will produce the outcome to "aux" folder
    #greasy_line += f"""python3 pipeline/pipeline.py  --input {os.path.join(INPUT_PDF_BASE_PATH,doc_name.rstrip())} --output {os.path.join(OUTPUT_GPFS_PROCESSED_PDFS_PATHS, doc_name.split('.pdf')[0]) }   --model {PATH_TO_PDF2TXT_MODEL}\n"""


    # We better organise it so the doc is insed its corresponding "ntpXXX" folder in the final destination path
    licitacion_id = doc_name.split('_')[0]
    name_without_extension = doc_name.split('.pdf')[0]
    output_final_path = os.path.join(OUTPUT_FINAL_ORGANISED_GPFS_PROCESSED_PDFS_PATHS,licitacion_id,name_without_extension )
    greasy_line += f"""python3 pipeline/pipeline.py  --input {os.path.join(INPUT_PDF_BASE_PATH,doc_name.rstrip())} --output {output_final_path}   --model {PATH_TO_PDF2TXT_MODEL}\n"""



    return greasy_line




def _get_line_greasy_with_several_docs(doc_names):
    '''To be called by _write_tasks_greasy_file func 
    
    '''

    # Theorically, the greasy env is inherited by the current python env
    #greasy_line = f'''[@ {PATH_TO_PDF2TXT} @] source {PATH_TO_PDF2TXT_ENV_SCRIPT}  && '''
    greasy_line = f'''[@ {PATH_TO_PDF2TXT} @] '''
    greasy_line += f"""python3 pipeline/pipeline.py --model {PATH_TO_PDF2TXT_MODEL} """



    for doc_name in doc_names:
        licitacion_id = doc_name.split('_')[0]
        name_without_extension = doc_name.split('.pdf')[0]
        output_final_path = os.path.join(OUTPUT_FINAL_ORGANISED_GPFS_PROCESSED_PDFS_PATHS,licitacion_id,name_without_extension )
        # per each doc:
        #   -l inputpath,outputpath
        greasy_line += f"""-l {os.path.join(INPUT_PDF_BASE_PATH,doc_name.rstrip())},{output_final_path} """


    greasy_line += f"""\n"""

    return greasy_line





def _write_tasks_greasy_file(script_folder_path, list_of_assigned_docs, args):
    n_docs_per_greasy_line = int(args.n_docs_per_greasy_line)
    # Only one doc per greasy line
    if(n_docs_per_greasy_line == 1):
        greasy_lines = [_get_line_greasy_for_doc(doc_name) for doc_name in list_of_assigned_docs]
    else:# several docs per each greasy line
        greasy_lines = [_get_line_greasy_with_several_docs(list_with_doc_names) for list_with_doc_names in chunk_list_in_n_slices(list_of_assigned_docs, n_docs_per_greasy_line) ]

    with open(os.path.join(script_folder_path, 'tasks.greasy'),'w') as f:
        f.write(''.join(greasy_lines))



def _create_subscript_files(    path_to_write_scripts, 
                                id_subscript, 
                                n_docs_per_launcher, 
                                list_of_assigned_docs,
                                args
                            ):


    # Script folder creation
    script_folder_name = f'launcher_n_{id_subscript}_to_process_{n_docs_per_launcher}_docs'
    script_folder_path = os.path.join(path_to_write_scripts, script_folder_name)
    os.makedirs(script_folder_path , exist_ok=True)
    os.makedirs( os.path.join(script_folder_path, 'logs'), exist_ok=True)



    # Launcher sh creation
    script_name = f'launcher_n_{id_subscript}_to_process_{n_docs_per_launcher}.sh'
    _write_launcher_sh_script(script_folder_path, script_name, id_subscript, args)

    # tasks.greasy creation
    _write_tasks_greasy_file(script_folder_path, list_of_assigned_docs, args)






def parse_args():

    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)


    parser.add_argument("-n", 
                        "--n_docs_per_launcher",
                        default=10000,
                        help="number of files that ssingle slurm launcher.sh script will take"
                        )

    parser.add_argument( "--n_docs_per_greasy_line",
                        default=1,
                        help="number of files that each single line in greasy file will process. Each line requieres load the entire pdf2txt pipeline in memory."
                        )
    

    parser.add_argument( "--cpus_per_task",
                        default=1,
                        help="--cpus-per-task slurm command " 
                        )
    
    parser.add_argument("--ntasks",
                        default=8,
                        help="--ntasks slurm command that will be written in all scripts " 
                        )

    parser.add_argument("-s", 
                        "--suffix",
                        default='',
                        help="suffix to add to the output folder e.g. '_amd_config' " 
                        )
    
    args = parser.parse_args()

    return args




def main(*args, **kwargs):


        
    args = parse_args(*args, **kwargs)
    n_docs_per_launcher = int(args.n_docs_per_launcher)
    os.makedirs(OUTPUT_GPFS_PROCESSED_PDFS_PATHS , exist_ok=True)

    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    logging.info(f'Starting with args: \n  {args}')

    

    logging.info(f'Reading list of files in {INPUT_PDF_LIST}...\n')
    with open(INPUT_PDF_LIST, 'r') as f:
        doc_list =  f.readlines()
    n_of_subscripts = math.ceil(len(doc_list)/n_docs_per_launcher)
    logging.info(f'Done!\n')
    logging.info(f'{n_of_subscripts} subscripts will be created, each of this one will process {n_docs_per_launcher} pdfs.\n')



    # Timestamp
    # datetime object containing current date and time
    now = datetime.now()
    timestamp_string = now.strftime("%Y_%m_%d_%Hh%Mm%Ss")

    # Create subscripts
    path_to_write_scripts = f'./subscripts/created_on_{timestamp_string}'
    if(args.suffix is not None):
        path_to_write_scripts += '_' + str(args.suffix)
    logging.info(f'Creating scripts files under {path_to_write_scripts}...\n')
    os.makedirs(path_to_write_scripts , exist_ok=True)
    list_slices = chunk_list_in_n_slices(doc_list, n_docs_per_launcher)
    [_create_subscript_files(path_to_write_scripts, i, n_docs_per_launcher, slice, args) for i,slice in enumerate(list_slices) ]    
    logging.info(f'Done!\n')



    #Create an excel to keep track among the 3 of us
    logging.info(f'Creating excel (.csv) to manually keep track of subscripts executions...\n')
    with open( os.path.join(path_to_write_scripts, 'tracker_of_subscripts_executions.csv'), 'w') as f:
        f.write(f'script name, assigned_to, has_been_launched, has_finished_execution, launched_on_cluster\n') #HEADER
        content = [f'launcher_n_{id_subscript}_to_process_{n_docs_per_launcher}.sh,,NO,NO,' for id_subscript in range(n_of_subscripts)]
        f.write('\n'.join(content))
    logging.info(f'Done!\n')




if __name__ == "__main__":
    main()





''' RUN
    python3 pdf2txt_massive_scripts_creator.py -n 10000

    Note: "subscripts" folders will appear in this directory :D
'''