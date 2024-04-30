from pdf_to_text import *
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import argparse
import sys
import os
import logging
import shutil




MAX_TOKENIZER_LENGTH=512


def process_doc(pdf_path, pipe, output_path, override = False):
    '''Processes and saves each pdf file'''

    # Create folder for output
    docname = os.path.basename(os.fsdecode(pdf_path)).split('.')[0]
    processed_output_folder= os.path.join(output_path,docname + '_processed')
    finished_processed_output_folder= os.path.join(output_path,docname + '_processed_finished')


    # Check doc is not already processed and if we want to override
    if  os.path.exists(finished_processed_output_folder):
        if(override):
            #logging.warning('Directory: ' + str(processed_output_folder) + ' already exits. Overwriting it!')
            shutil.rmtree(finished_processed_output_folder)           # Removes all the subdirectories!
        else:
            return 
        

    # Create temp folder
    if  os.path.exists(processed_output_folder):
        # If we are here, something went wrong so we overwrite files
        shutil.rmtree(processed_output_folder)           # Removes all the subdirectories!
    os.makedirs(processed_output_folder)


    all_paragraphs = get_paragraphs_from_pdf(pdf_path)

    with open(os.path.join(processed_output_folder, 'Doc_labeled_sections.txt'), 'w') as f:

        for page_number,  paragraphs_in_page in enumerate(all_paragraphs):
            buffer_page_to_file = []

            #f.write('## PAGE:'+ str(page_number) + '##\n\n')
            buffer_page_to_file.append('## PAGE:'+ str(page_number) + '##\n\n')
            for text, coords, is_table in paragraphs_in_page:
                if not is_table: #Process if not table
                    label = pipe((text[MAX_TOKENIZER_LENGTH]) if len(text) > MAX_TOKENIZER_LENGTH else text)
                    #f.write('#' + str(label[0]['label']).upper().strip() + ':\n' + text.rstrip() + '\n')
                    buffer_page_to_file.append('#' + str(label[0]['label']).upper().strip() + ':\n' + text.rstrip() + '\n')
                else:
                    label = "Table"
                    #f.write('#' + label.upper() + ':\n' + '...\n')
                    buffer_page_to_file.append('#' + label.upper() + ':\n' + '...\n')


            f.write(''.join(buffer_page_to_file))

    # Rename folder in order to reflext the process is finished
    os.rename(processed_output_folder, finished_processed_output_folder)



def _process_doc_ (input_pdf_path_and_output_pdf_path, pipe, override_output):
    '''Aux func that calls process_doc() when invoked from -l arg and allows to be called easily by a
        comprehension list
    
    
    '''

    input_pdf_path, output_pdf_path = input_pdf_path_and_output_pdf_path.split(',')

    filename = os.fsdecode(input_pdf_path)
    if filename.endswith(".pdf"): 
        try:
            process_doc(os.path.join(input_pdf_path), pipe, output_pdf_path, override_output)
        except Exception as e:
            err_msg = "ERROR: An error ocurred parsing the document called: " + filename + '. \n\t' + str(e)
            #raise  Exception( err_msg)
            print(err_msg)
    else:
        logging.info('Skipping non-pdf file: ' + filename)





# Define a custom argument type for a list of strings
def list_of_strings(arg):
    return arg.split(',')




def main(*args, **kwargs):


    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)



    parser.add_argument("-i", 
                        "--input",
                        default=None,
                        help="PDF doc or directory with several pdf documents"
                        )
    

    parser.add_argument('-l',
                        '--list_of_inputs_and_outputs', 
                        action='append', 
                        default=None,
                        help='List of docs to process. Each element must be preceded by the argument mark. Each pair input,output must be separated by commas.',
                        )

    
    parser.add_argument("-o", 
                        "--output",
                        help="Output path."
                        )


    parser.add_argument("-m", "--model", 
                        default="https://huggingface.co/BSC-LT/NextProcurement_pdfutils",
                        help="Path to used doc-processing model."
                        )
    
    parser.add_argument('--override_output',
                        action="store_true",
                        help='if set overrides the output'
                        )


    if len(sys.argv)==1:
        parser.print_help(sys.stderr)
        sys.exit(1)


    args = parser.parse_args()


    if(args.list_of_inputs_and_outputs is None and args.input is None):
        err_msg = "ERROR: --input or  --list_of_inputs_and_outputs argument must be provided!"
        raise  Exception( err_msg )


    if(args.output is None and args.list_of_inputs_and_outputs is None):
        err_msg = "ERROR: --output path must be provided!"
        raise  Exception( err_msg )

     
    # Load model and pipeline
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    tokenizer = AutoTokenizer.from_pretrained(str(args.model))
    model = AutoModelForSequenceClassification.from_pretrained(str(args.model))
    #pipe = pipeline("text-classification", model=model, tokenizer=tokenizer)
    pipe = pipeline("text-classification", model=model, tokenizer=tokenizer, device = -1)


    # List of N docs to process (helpul when we want to paralellize)
    if(args.list_of_inputs_and_outputs is not None):
        # Process the list with a comprehension list :)
        [ _process_doc_(input_pdf_path_and_output_pdf_path, pipe, args.override_output) for input_pdf_path_and_output_pdf_path in args.list_of_inputs_and_outputs]

    # An folder with docs
    elif(os.path.isdir(args.input) ):
        for pdf_path in os.listdir(args.input):
            filename = os.fsdecode(pdf_path)
            if filename.endswith(".pdf"): 
                try:
                    process_doc(os.path.join(args.input,pdf_path), pipe, args.output, args.override_output)
                except Exception as e:
                    err_msg = "ERROR: An error ocurred parsing the document called: " + filename + '. \n\t' + str(e)
                    #raise  Exception( err_msg)
                    print(err_msg)
            else:
                logging.info('Skipping non-pdf file: ' + filename)
       
    else:#single input doc
        process_doc(args.input, pipe, args.output, args.override_output)





  
    
    


if __name__ == "__main__":
    main()



'''Run example:



    # With a list of files:

        > python3 pipeline/pipeline.py  --model <INTERNAL GPFS PATH>NextProcurement/model_trainer/output/NextProcurement/next_procurement_v0_8_0.00005_date_22-08-13_time_04-51-37  \
            -l <INTERNAL PATH>data/ntp01067774_Pliego_Prescripciones_tecnicas_URI.pdf,<INTERNAL PATH>_ttest_tuple_input_output/1    \
            -l <INTERNAL PATH>data/ntp01024989_Documento_Publicado_URI:5.pdf,<INTERNAL PATH>_ttest_tuple_input_output/2



'''