'''Dado un archivo de index, me procesa las cosas con pdf2txt

'''


import re
import json
import os


#FILENAME = 'index_head200.csv'
FILENAME = 'index_from_200.csv'


#GPFS_ORIGINAL_PDFS_PATHS = '''/home/arubio/bsc/dt<INTERNAL PATH>data'''
#GPFS_OUTPUT_PDFS_PATHS = '''/home/arubio/bsc/dt<INTERNAL PATH>data_processed_organised'''



GPFS_ORIGINAL_PDFS_PATHS = '''<INTERNAL PATH>data'''
GPFS_OUTPUT_PDFS_PATHS = '''<INTERNAL PATH>data_processed_organised'''




pattern = r"ntp\d+_.*\.pdf"

def get_pdf_names(input_file):

    with open(input_file ,'r') as f:
        for i, l in enumerate(f):
            if(i==0):
                continue
            pdf_names = re.findall(pattern, l)
            if(len(pdf_names) > 0):
                #print(pdf_names)
                clean_pdf_names = pdf_names[0].replace('\'','').split(',')
                clean_pdf_names = [i.strip() for i in clean_pdf_names ]
                #print(clean_pdf_names)
                #print('-------------------------')

                yield clean_pdf_names
        
#get_pdf_names(FILENAME)






GREASY_EXEC_PATH= '[@ <INTERNAL GPFS PATH>NextProcurement_v2 @]'
GREASY_VENV_CMD= 'source <INTERNAL GPFS PATH>NextProcurement_v2/scripts/venvs/use_env_amd.sh'

greasy_cmd = f'python3 pipeline/pipeline.py --input {os.path.join(GPFS_ORIGINAL_PDFS_PATHS, )} --output {os.path.join(GPFS_OUTPUT_PDFS_PATHS, )} --model <INTERNAL GPFS PATH>NextProcurement/model_trainer/output/NextProcurement/next_procurement_v0_8_0.00005_date_22-08-13_time_04-51-37'




with open('tasks.greasy', 'w+') as f:
    # Write to greasy commmand for pdf2txt
    for proc_pdf_list in get_pdf_names(FILENAME):

        licitacion_id = proc_pdf_list[0].split('_')[0]
        #print(f' {licitacion_id} with {len(proc_pdf_list)} docs')
        for single_pdf_name in proc_pdf_list:

            name_without_extension = single_pdf_name.split('.')[0]
            greasy_cmd = f'python3 pipeline/pipeline.py --input {os.path.join(GPFS_ORIGINAL_PDFS_PATHS,single_pdf_name )} \
                    --output {os.path.join(GPFS_OUTPUT_PDFS_PATHS,licitacion_id,name_without_extension )} \
                    --model <INTERNAL GPFS PATH>NextProcurement/model_trainer/output/NextProcurement/next_procurement_v0_8_0.00005_date_22-08-13_time_04-51-37'

            complete_greasy_cmd = f'{GREASY_EXEC_PATH} {GREASY_VENV_CMD} && {greasy_cmd}'
            #print(complete_greasy_cmd)
            f.write(f'{complete_greasy_cmd}\n')


        #break
