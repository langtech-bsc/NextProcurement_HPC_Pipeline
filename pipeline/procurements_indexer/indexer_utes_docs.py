''' Given a list of UTES in a parquet file, generates a csv file with the procurements and its 
        attached pdf docs (grouped by procurement)


    WARNING!:
        THIS SCRIPTS NEEDS INTERNET CONNECTION!

            So... cannot be executed under clusters!

'''


import sys
import os
import argparse
import pyarrow.parquet as pa
import pandas as pd
import logging

import client_res_api




GPFS_ORIGINAL_PDFS_PATHS = '''/home/arubio/bsc/dt<INTERNAL PATH>data'''
#GPFS_ORIGINAL_PDFS_PATHS = '''<INTERNAL PATH>data'''
GPFS_PROCESSED_PDFS_PATHS = '''<INTERNAL GPFS PATH>NextProcurement/pipeline_executions/nextproc_17oct23_amd/data_processed'''



def get_ute_info_from_parquet_file(input_file):
    '''
        Args:
            input_file: parquet input file
    
            
        Returns:
            ute_info_dict:  {ute_NIF: (id_tender, Name)}
    '''

    logging.info(f'Reading file: {input_file}')

    table = pa.read_table(input_file) 
    df = table.to_pandas() 
    df = df.loc[:, ['NIF','id_tender', 'Name']]

    ute_info_dict = {}

    for index, row in df.iterrows():
        ute_info_dict[row['NIF']] = (row['id_tender'],row['Name'] )


    logging.info(f'File loaded: {input_file}')


    return ute_info_dict






def list_pdf_directory_in_gpfs(directory, args, save = False):

    ''' Returns a list with pdf names in the given directory
    
    '''

    list_of_files = []
    logging.info(f'\t Asking OS to list dir...')
    dis_list = os.listdir(directory)
    logging.info(f'\t Done!...')


    if(save):        
        with open(os.path.join('INDEX_UNPROCESSED_FILES/','gpfs_index.txt'), 'w') as f:
            for pdf_path in dis_list:
                filename = os.fsdecode(pdf_path)
                if filename.endswith(".pdf"): 
                    list_of_files.append(filename)
                    f.write(f'{os.path.join(directory, filename)}\n' )
                    #debug
                    f.flush()
                    print(filename)


        logging.info(f'\tSaved!')


    else:
        for pdf_path in dis_list:
            filename = os.fsdecode(pdf_path)
            if filename.endswith(".pdf"): 
                list_of_files.append(filename)
      


    #if(save):        



    return list_of_files




def get_files_from_place_id(list_of_files, place_id):
    'Return list with matching filenames'
    return [i.rstrip() for i in list_of_files if place_id in i]






def parse_args():

    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)



    parser.add_argument("-i", 
                        "--input",
                        default='UTES_INFO_FILES/utes.parquet',
                        help="Parquet input file with UTEs."
                        )
    
        
    parser.add_argument("-o", 
                        "--output",
                        default='INDEX_PROCUREMENTS/',
                        help="Output Folder where .csv procurements index will be written (also some stats)."
                        )
    

            
    parser.add_argument("--list_of_pdfnames_file",
                        #default='INDEX_UNPROCESSED_FILES/gpfs_index.txt',
                        default='ll_processer/cleaned_index.txt',
                        help=" Providing manually the list of PLACE unprocessed ones (one doc name per line)"
                        )
    

    parser.add_argument("--save_list_of_pdfnames",
                        action='store_true',                        
                        default=True,
                        help="Flag. If list_of_pdfnames_file not given and this arg is provided with a path, the list of pdf NAMES \
                              will be saved under pdf_dirlist.txt"
                        )

    
    

    if len(sys.argv)==1:
        parser.print_help(sys.stderr)
        sys.exit(1)


    args = parser.parse_args()

    return args






def main(*args, **kwargs):


    args = parse_args(*args, **kwargs)



    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    logging.info(f'Starting with args: \n  {args}')


    # List of input files (COMPLETE PATH WILL BE GPFS_ORIGINAL_PDFS_PATHS + [i for i in list_of_files] )
    # list_of_files contais a list of names
    if(args.list_of_pdfnames_file is not  None):
        list_of_files = []
        logging.info(f'Loading pdf names from file {args.list_of_pdfnames_file}')
        with open(args.list_of_pdfnames_file,'r') as f:
            for l in f:
                list_of_files.append(l)
        logging.info(f'Loaded {len(list_of_files)} pdf names from file {args.list_of_pdfnames_file}')

    else:
        logging.info(f'Listing files in  directory {GPFS_ORIGINAL_PDFS_PATHS} ...')
        list_of_files =  list_pdf_directory_in_gpfs(GPFS_ORIGINAL_PDFS_PATHS,args, args.list_of_pdfnames_file)
        logging.info(f'DONE! Listed {len(list_of_files)} pdf names from directory {GPFS_ORIGINAL_PDFS_PATHS}')






    # UTES INFO
    ute_info_dict = get_ute_info_from_parquet_file(args.input)


    logging.info(f'WRITTING {args.output} ...')
    n_licitaciones_without_docs = 0
    n_licitaciones_total = 0    
    n_total_of_indexed_docs=0

    #err stats
    n_err_none_placeid = 0
    list_err_none_placeid = []


    # Write Index 
    os.makedirs(args.output,exist_ok = True)
    with open(os.path.join( args.output,'index.csv'),'w') as f_index:
    #with open(args.output,'w', buffering = 1) as f_index:

        

        # HEADER
        f_index.write('UTE_ID,\t\tUTE_CLEAN_NAME,\t\tPROCUREMENT_INTERNAL_ID,\t\tLIST_OF_DOCS \n')


        for ute_nif in ute_info_dict.keys():

            #print(f'------\n\n\nUTE NIF: {ute_nif}')

            ute_procurements_place_ids =  client_res_api.get_procurements_place_ids_from_ute_nif(ute_nif)  
            ute_procurements = {}
            _, ute_clean_name = ute_info_dict[ute_nif]
            for place_id in ute_procurements_place_ids:

                # err handling from API
                if place_id is None:
                    n_err_none_placeid += 1
                    list_err_none_placeid.append(f'{ute_nif},\t\t{ute_clean_name},\t\t{place_id},\t\t{[]}\n')
                    continue

                # Getting files
                licitacion_docs_filenames= get_files_from_place_id(list_of_files, place_id)
                ute_procurements[place_id] = licitacion_docs_filenames

                #print(f'\t\t LICITACIÓN ID: {place_id}')
                #print(f'\t\t Docs asociados ID: {licitacion_docs_filenames}')

                f_index.write(f'{ute_nif},\t\t{ute_clean_name},\t\t{place_id},\t\t{licitacion_docs_filenames}\n')

                # Stats
                n_licitaciones_total += 1
                if(len(licitacion_docs_filenames)==0):
                    n_licitaciones_without_docs += 1
                else:
                    n_total_of_indexed_docs += len(licitacion_docs_filenames)

                if(n_licitaciones_total % 50 == 0):
                    print(f'\t\tProcessed a total of {n_licitaciones_total}...')


    logging.info(f'Done!')



    # STATS
    logging.info(f'WRITTING stats to {args.output} ...\n')
    with open(os.path.join( args.output,'stats_gpfs_index.txt'),'w') as f:
        f.write(f'STATS\n')
        f.write(f'STATS\nNumber of procurements without any pdfs in gpfs: {n_licitaciones_without_docs} of a total of {n_licitaciones_total} procurements.\n')
        f.write(f'STATS\nNumber docs listed in this index:{n_total_of_indexed_docs} of a total of {len(list_of_files)} given in gpfs index.\n')
    # Err Stats
    with open(os.path.join( args.output,'err_stats_gpfs_index.txt'),'w') as f:
        f.writelines('\n'.join(list_err_none_placeid))
    logging.info(f'Done!')




if __name__ == "__main__":
    main()



''' RUN
    # OLD PATHS
    #########python index_utes_docs.py -i UTES_INFO_FILES/utes.parquet -o INDEX_PROCUREMENTS --list_of_pdfnames_file INDEX_UNPROCESSED_FILES/gpfs_index.txt

    
    # NEW RELATIVE PATHS (UPDATED)
    python3 indexer_utes_docs.py -i ../INDEXES/UTES_INFO_FILES/utes.parquet -o ../INDEXES/TAGGED_PROCUREMENTS --list_of_pdfnames_file ../INDEXES/ORIGINAL_PDF_LIST/cleaned_index.txt

  
'''