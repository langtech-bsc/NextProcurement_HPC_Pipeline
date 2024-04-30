'''The purpose of this script is to read the tree of subscripts subdirectories within the 
    massive_corpus_porcesser module, in order to find the documents whose execution
    failed throughout the 3 clusters.

    Since many jobs where needed to be created and launched, there are many diferents
    logs files. In addition, this logs contains 'batches' of diferents docs. This makes
    the tracking of errors a nightmare.


    This way, the script will automatically automatically read and  collect all the documents that are still required
    to be processed with pdf2txt (through massive_corpus_processer). The output of the script will be another
    "cleaned_index.txt" (see workflow diagram) so that in the next launching, massive_corpus_processer does not
    need to check one by one all of the files if they are already processed, which will make the process of "relaunching"
    way too slow...




'''

import os
import logging
import glob

import sys


SYTEM_ROOT_FOLDER_WITH_SUBSCRIPTS= '../subscripts/'

LIST_OF_SUBSCRIPTS_FOLDER_TO_READ = [   
                                        'created_on_2024_04_17_10h31m26s_mn',
                                        'created_on_2024_04_17_10h30m50s_nord3',
                                        'created_on_2024_04_17_10h30m10s_amd',

                                        'created_on_2024_04_13_01h50m42s_mn/',
                                        'created_on_2024_04_13_01h50m20s_nord3/',
                                        'created_on_2024_04_13_01h49m58s_amd/',




                                    ]


RE_CLEANED_INDEX_OUTPUT_FOLDER_PATH = '''<INTERNAL BSC PATH>/NextProcurement_dev/pipeline/INDEXES/ORIGINAL_PDF_LIST/RELAUNCH_DOCS_STILL_PENDING/'''


# NOW THE CURRENT CONF. FOR ALL FOLDERS IS: 0-425 SUBSCRIPTS
N_OF_SUBSCRIPTS_PER_CLUSTER= 426
N_DOCS_PER_SUBSCRIPT= 10000


GREASY_TASK_SPLIT_MARK = 'next_procurement_v0_8_0.00005_date_22-08-13_time_04-51-37'


def _read_greasy_restart_file(filepath):
    '''Reads a greasy restart file and returs the number and list of paths of
        failed docs processed with massive_corpus_processer 
    
    '''

    n_of_failed_tasks = 0
    failed_docs_original_paths = []

    try:
        with open(filepath, 'r') as f:
            for l in f:
                #if('# Warning:' in l): # Indicator of failed task
                #    n_of_failed_tasks +=1
                #elif('[@  /gpfs/' in l): # Collecting failed task command
                if('[@  /gpfs/' in l):
                    n_of_failed_tasks +=1
                    clean_task = l.split(GREASY_TASK_SPLIT_MARK)[1]
                    docs_lists = clean_task.split('-l')[1:]
                    for doc_path in docs_lists:
                        failed_docs_original_paths.append(doc_path.split(',')[0].lstrip())

    except Exception as e:
        logging.info(f'Unable to read file: {filepath} \n\tException:{e}')



    

    return n_of_failed_tasks, failed_docs_original_paths




def _process_one_final_subscript_folder(path, env_name):
    '''

              e.g. --> folder_path = <INTERNAL BSC PATH>/NextProcurement_dev/pipeline/pdf2txt/massive_corpus_processer/subscripts/created_on_2024_04_17_10h31m26s_mn/launcher_n_320_to_process_10000_docs/

    '''

    n_of_failed_tasks = 0
    failed_docs_original_paths = []



    # TODO DELETE DEBUG
    #print(f'env_name:{env_name}')

    if(env_name == 'amd'):

        greasy_rst_file_path = os.path.join(path, 'logs/greasy_restartfile.log')        
        if(os.path.exists(greasy_rst_file_path)  ):

            # TODO DELETE DEBUG
            print(f'Catched rst file(amd):{greasy_rst_file_path}')


            n_of_failed_tasks, failed_docs_original_paths = _read_greasy_restart_file(greasy_rst_file_path)

        #else:
            # No errors then


    else: # nord3 or mn

        for greasy_rst_file_path in glob.glob( os.path.join(path, 'tasks.greasy*.rst')): #if there are any reset files

            # TODO DELETE DEBUG
            print(f'Catched rst file(no-amd):{greasy_rst_file_path}')

            n_of_failed_tasks, failed_docs_original_paths = _read_greasy_restart_file(greasy_rst_file_path)


    return n_of_failed_tasks, failed_docs_original_paths





def process_whole_subdirectory_folder(root_folder_path):
    '''This func. will process an entire folder
        created for a specific cluster.

        Each path-folder will have 'launcher_n_XX_to_process_YY_docs'
          as a final pathname


          e.g. --> folder_path = <INTERNAL BSC PATH>/NextProcurement_dev/pipeline/pdf2txt/massive_corpus_processer/subscripts/created_on_2024_04_17_10h31m26s_mn/
    '''


    # Getting env from folder name
    env_name = root_folder_path.split('created_on')[1].split('/')[0].split('_')[-1]

    # #Processing all 0 to N (426) subfolders
    #subdirectory_info = [  _process_one_final_subscript_folder(os.path.join(root_folder_path, f'launcher_n_{i}_to_process_{N_DOCS_PER_SUBSCRIPT}_docs'), env_name)  for i in range(N_OF_SUBSCRIPTS_PER_CLUSTER) ]
    # Info and stats
    #n_of_failed_tasks_total = sum([i for i,_ in subdirectory_info])
    #failed_docs_original_paths_total = [i for _,i in subdirectory_info]

    # Processing all 0 to N (426) subfolders
    n_of_failed_tasks_total = 0
    failed_docs_original_paths_total = []
    for i in range(N_OF_SUBSCRIPTS_PER_CLUSTER):
        script_name =  f'launcher_n_{i}_to_process_{N_DOCS_PER_SUBSCRIPT}_docs' # we dont want the sh, we want the folder
        n, listt =  _process_one_final_subscript_folder( os.path.join(root_folder_path, script_name), env_name) 
        n_of_failed_tasks_total += n
        failed_docs_original_paths_total.extend(listt)

  

    # TODO DELETE DEBUG
    #print('test')
    #for i in failed_docs_original_paths_total:
    #    print(i)
    sys.stdout.flush()

    #[print(i) for i in failed_docs_original_paths_total]


    return n_of_failed_tasks_total, failed_docs_original_paths_total



def main(*args, **kwargs):

    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    logging.info(f'Starting with args: \n  {args}')

    # Common between all clusters (name convention)
    logs_subfolder_names = [f'launcher_n_{i}_to_process_{N_DOCS_PER_SUBSCRIPT}_docs/logs/' for i in range(N_OF_SUBSCRIPTS_PER_CLUSTER)]
    
    executions_info =  [process_whole_subdirectory_folder(os.path.join(SYTEM_ROOT_FOLDER_WITH_SUBSCRIPTS, i) ) for i in LIST_OF_SUBSCRIPTS_FOLDER_TO_READ  ]


    # Info and stats
    n_of_failed_tasks_total = sum([i for i,_ in executions_info])
    failed_docs_original_paths_total = []
    #failed_docs_original_paths_total = [failed_docs_original_paths_total.extend(i) for _,i in executions_info]
    for _,i in executions_info:
        failed_docs_original_paths_total.extend(i)
    #failed_docs_original_paths_total = [i for _,i in executions_info]

    logging.info(f'Global number of greasy tasks failed: {n_of_failed_tasks_total} \n')
    logging.info(f'Global number of pdfs pending to be processed still: {len(failed_docs_original_paths_total)} \n')


    cleaned_index_path = os.path.join(RE_CLEANED_INDEX_OUTPUT_FOLDER_PATH, 'cleaned_index.txt')
    logging.info(f'\nGenerating new cleaned_index in: {cleaned_index_path} ...\n')
    with open(cleaned_index_path, 'w')as fout:
        fout.write('\n'.join(failed_docs_original_paths_total))
    logging.info(f'Done!:\n')



if __name__ == "__main__":
    main()
