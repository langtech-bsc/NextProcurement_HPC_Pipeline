'''Takes pdf2txt texts and generates a .parquet file

    
    DISCLAIMER: ONLY FOR BSC INTERNAL USE!!


    NOTE: Compression level can be change in order to improve speed/disk usage


'''
import sys
import os
from datetime import datetime
import time
import logging
#from fastparquet import write
import pandas as pd

# MACROS
# Test using the sample folder
#GPFS_OUTPUT_PDFS_PATHS = '''<INTERNAL PATH>data_processed_organised_sample'''
GPFS_OUTPUT_PDFS_PATHS = '''<INTERNAL PATH>data_processed_organised'''

PARQUET_OUTPUT_PATH = '''<INTERNAL PATH>data_processed_organised_parquet/procurements.parq'''
LOGS_PATH = './logs/parquet_creator_log.txt'






def list_procurements_folders(directory):

    ''' Returns a list with ntpXXX folders in the given directory
    
    '''

    logging.info(f'Listing folder in  directory {GPFS_OUTPUT_PDFS_PATHS} ...')
    dis_list = [ f'{GPFS_OUTPUT_PDFS_PATHS}/{i}'  for i in  os.listdir(directory)]
    logging.info(f'DONE! Listed {len(dis_list)} procurements.')

    return dis_list

def _process_doc_folder(ntp_folder_path,doc_name, ntp_id):
    '''REcieves a document inside ntpxxxx folder.
        Returns:

            (ntp_id,doc_name, txt) tuple if everything ok
            None if something went wrong
    
    '''

    doc_txt = None

    #doc_name = os.path.basename(ntp_folder_path)

    # REading Doc_labeled_sections.txt
    pdf_processed_subfolder_name = doc_name + '_processed_finished'
    pdf_folder_txt_path = os.path.join(ntp_folder_path, doc_name, pdf_processed_subfolder_name)
    if(os.path.exists(pdf_folder_txt_path) ):
        txt_path = os.path.join(pdf_folder_txt_path,'Doc_labeled_sections.txt' )
        with open(txt_path, 'r') as f:
            doc_txt =  ''.join(f.readlines())

        return (ntp_id,doc_name,doc_txt)
    else:
        logging.warning(f'Err Cannot find path:{pdf_folder_txt_path}\n\t Skipping doc...')
        return None



    

def _process_procurement_folder(proc_path):
    '''REcieves a ntpxxxx folder
        Returs:
            [(ntp_id,doc_name, txt) )] with al procurement content found
    '''
    
    ntp_id = os.path.basename(proc_path)

    # Paths handling
    dir_content =  os.listdir(proc_path)
    #print(f'dir_content:\n{dir_content}')
    doc_names = [ i for i in  dir_content if os.path.isdir(os.path.join(proc_path, i)) ]
    #print(f'docs_paths:\n{docs_paths}')

    #Processing
    all_proc_result = [_process_doc_folder(proc_path,doc_name, ntp_id) for doc_name in doc_names if _process_doc_folder(proc_path,doc_name, ntp_id) is not None]
    #print(f'all_proc_result:\n{all_proc_result}')
    return all_proc_result  




def main(*args, **kwargs):

    # Logger setup
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    logging.info(f'Starting statistics calculator with args: \n  {args}')



    t_0 = time.time()
    list_of_files =  list_procurements_folders(GPFS_OUTPUT_PDFS_PATHS)
    n_of_procurements = len(list_of_files)
    t_1 = time.time() 


    logging.info(f'Starting parquet creation')


    # level_of_relevance 1: Low
    #                      2: High
    #'level_of_relevance': 'int',
    columns = {
        'procurement_id': 'string',
        'doc_name' : 'string',
        'content': 'string'  # This column will contain long text
    }



    # Get results of each procuremetn
    # [ _process_procurement_folder(ntp) for ntp in list_of_files]

    unfolded_list =  []
    [ unfolded_list.extend(_process_procurement_folder(ntp)) for ntp in list_of_files]
 
    #print('---')
    ##print(unfolded_list)
    #for e in unfolded_list:
    #    print(e)

    df_content = [ {'procurement_id': ntp_id, 'doc_name':doc_name,  'content': txt } for (ntp_id, doc_name, txt) in  unfolded_list]
    t_2 = time.time() 
    logging.info(f'Finished doc processing... Starting conversion')
    df = pd.DataFrame(columns=columns)
    df = df.append(df_content)

    # Writing parquet
    t_3 = time.time() 
    logging.info(f'Creating parquet file')
    os.makedirs(os.path.dirname(PARQUET_OUTPUT_PATH),exist_ok = True)
    #write(PARQUET_OUTPUT_PATH, df) # C MEMORY ERROR
    df.to_parquet(  PARQUET_OUTPUT_PATH, 
                    #engine='fastparquet', 
                    engine='pyarrow', 
                    compression='lz4'
                  )
    t_4 = time.time() 
    logging.info(f'Finished!')




    # Write stats 
    with open(LOGS_PATH, 'w') as f:
        f.write('PARQUET CREATION STATISTICS:\n')

        # Timestamp  
        now = datetime.now() # current date and time
        date_time = now.strftime("%Y_%m_%d_%H:%M:%S")
        f.write(f'Finish Timestamp: {date_time}\t\n')



        f.write('STATS:\n')
        f.write(f'Number of docs added to parquet file shape {df.shape[0]}\n\n')

        time_spent_1 =  (t_1 - t_0)/60.
        f.write(f'\t\t ---> Time spent to list dir: {time_spent_1:4f} (min)\n')


        time_spent_2 =  (t_2 - t_1)/60.
        f.write(f'\t\t ---> Time spent read all files: {time_spent_2:4f} (min)\n')


        time_spent_3 =  (t_3 - t_2)/60.
        f.write(f'\t\t ---> Time spent to create pandas df: {time_spent_2:4f} (min)\n')

        time_spent_4=  (t_4 - t_3)/60.
        f.write(f'\t\t ---> Time spent to create parquet file from df: {time_spent_2:4f} (min)\n')


if __name__ == "__main__":
    main()




'''
    python parquet_creator.py
'''