'''Given the list of files downloaded from Swift sync (list or if not given python will get the list of files)
    this script will give 2 outputs:
        - File with the names of pdfs found in that directory
        - File with the names of non pdfs found in that directory (which wont be used for the rest of the Nextprocurement Pipeline)


 '''


import os


# MACROS
PATH_DOCS_SYNCHED_FROM_SWIFT = 'gpfs/home/bsc88/bsc88621execution_logs/list_original_docs_stdout-627494.out'
PATH_TO_FILE_WITH_LIST_OF_FILES = '../../statistics/pdf2txt_statistics_calculator/slurm_logs/list_of_original_pdfs_in_gpfs.out'



names = []
invalid_names= []



if ( os.path.exists(PATH_TO_FILE_WITH_LIST_OF_FILES) ):

    print(f'List of files will be taken from file: {PATH_TO_FILE_WITH_LIST_OF_FILES}')
    with open(PATH_TO_FILE_WITH_LIST_OF_FILES, 'r') as f:
        for i,l in enumerate(f):
            if(i==0):
                continue
            else:
                name = l.split()[-1]
                if('.pdf' in name):
                    names.append(name)
                else:
                    invalid_names.append(name)

else:
    print(f'List of files not found under {PATH_TO_FILE_WITH_LIST_OF_FILES}.\n \
          This script will list files under {PATH_DOCS_SYNCHED_FROM_SWIFT} ')
    
    print('Listing folder...')
    dirlist = os.listdir(PATH_DOCS_SYNCHED_FROM_SWIFT)
    print('Done!')

    for i,l in enumerate(dirlist):
        if(i==0):
            continue
        else:
            name = l.split()[-1]
            if('.pdf' in name):
                names.append(name)
            else:
                invalid_names.append(name)








print(f'Number or names processed:{i+1}')
print(f'\t-->Number of valid pdfs:{len(names)}')
print(f'\t-->Number of invalid documents (non-pdfs):{len(invalid_names)}')

with open('cleaned_index.txt', 'w') as f:
    #f.writelines('\n'.join(names) + '\n')
    f.writelines('\n'.join(names))


with open('cleaned_index_invalid_names.txt', 'w') as f:
    #f.writelines('\n'.join(invalid_names) + '\n')
    f.writelines('\n'.join(invalid_names))
