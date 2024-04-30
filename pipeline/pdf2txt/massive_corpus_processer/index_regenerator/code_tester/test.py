import glob


GREASY_TASK_SPLIT_MARK = 'next_procurement_v0_8_0.00005_date_22-08-13_time_04-51-37'


def _read_greasy_restart_file(filepath):
    '''Reads a greasy restart file and returs the number and list of paths of
        failed docs processed with massive_corpus_processer 
    
    '''

    n_of_failed_tasks = 0
    failed_docs_original_paths = []

    with open(filepath, 'r') as f:
        for l in f:
            #if('# Warning:' in l): # Indicator of failed task
            #    n_of_failed_tasks +=1
            #elif('[@  /gpfs/' in l): # Collecting failed task command
            if('[@  /gpfs/' in l):
                n_of_failed_tasks +=1
                clean_task = l.split(GREASY_TASK_SPLIT_MARK)[1]
                docs_lists = clean_task.split('-l')[1:]
                #print('docs_lists')
                #print(docs_lists)
                #return 
            

                for doc_path in docs_lists:
                    failed_docs_original_paths.append(doc_path.split(',')[0].lstrip())

    return n_of_failed_tasks, failed_docs_original_paths




for greasy_rst_file_path in glob.glob('tasks.greasy*.rst'): #if there are any reset files
    print(f'filename:{greasy_rst_file_path}')

    n_of_failed_tasks, failed_docs_original_paths = _read_greasy_restart_file(greasy_rst_file_path)

    print(f'n:{n_of_failed_tasks}')
    print(f'number of FAILED docs:{len(failed_docs_original_paths)}')


    l = '\n'.join(failed_docs_original_paths)
    print(f'failed_docs_original_paths:{l}')
