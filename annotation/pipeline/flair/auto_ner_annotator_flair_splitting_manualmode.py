'''Auto NER UTES in relevant procurements'''



import argparse
import os
import sys
from transformers import pipeline, AutoTokenizer, AutoModelForTokenClassification,logging
import torch
import logging as os_logging
import re
import json

import os
import flair




#######################################################
#                   FLAIR MACROS
#######################################################

## MACROS
#       changing cache dir to make flair model accesible to anyone in clusters :D

# hg cache dir
os.environ['TRANSFORMERS_CACHE'] = '<INTERNAL BSC PATH>NER_test/ner_models/cache_dir/huggingface'
print(f'''os.environ[TRANSFORMERS_CACHE'] : {os.environ['TRANSFORMERS_CACHE']}''')
os.environ['HF_HOME'] = '<INTERNAL BSC PATH>NER_test/ner_models/cache_dir/huggingface'
os.environ['HF_DATASETS_CACHE'] = '<INTERNAL BSC PATH>NER_test/ner_models/cache_dir/huggingface'

os.environ['TRANSFORMERS_OFFLINE'] = '1'
os.environ['HF_DATASETS_OFFLINE'] = '1'

# TODO CAN THIS BE AN STRING?
flair.cache_root = '<INTERNAL BSC PATH>NER_test/ner_models/cache_dir/flair'
print(f'flair.cache_root : {flair.cache_root }')

# Works without the link but a little bit slower because of scrath (seems like)
#os.system('ln -s <INTERNAL BSC PATH>NER_test/ner_models/cache_dir/huggingface ~/.cache/huggingface')
#os.system('ln -s <INTERNAL BSC PATH>NER_test/ner_models/cache_dir/flair ~/.cache/falir')


#######################################################





MODEL = "flair/ner-spanish-large"
logging.set_verbosity_error()




# INTERNAL MACROS
PATTERN = re.compile("report_relevant_pdf_list_from_nif_*")

#report_relevant_pdf_list_from_nif_u01656073.txt


#TODO HANDLEAR LOS ENTITUS_GROUPS

def format_ner_spans(   sentence, 
                        token_count_offset = 0, 
                        token_char_offset = 0,
                        ):
    '''Takes the output of the ner model and formats it to prodigy expected one
    
        Return
            List of dics, with spanss in prodigy format

            start, end --> char level
            token_start, token_end --> token count in sentence (0 first token)
    '''

    spans = []

    for t in sentence.get_spans('ner'):

        ## falta token_start y end
        if 'org' in t.tag.lower() :
            span = {}
            span['text']= t.text
            #span['label']= 'organization'
            span['label']= 'ORG'
            span['start']= t.start_position + token_char_offset
            span['end']= t.end_position + token_char_offset
            #Token count info retrieval
            pos = str(t).lstrip('Span[').rstrip(']:').split(':')
            span['token_start']=  int(pos[0])  + token_count_offset
            span['token_end']= int(pos[1].rstrip(']')) -1    + token_count_offset

            spans.append(span)

        elif 'misc' in  t.tag.lower():
            span = {}
            span['text']= t.text
            #span['label']= 'other'
            span['label']= 'MISC'
            span['start']= t.start_position + token_char_offset
            span['end']= t.end_position + token_char_offset
            #Token count info retrieval
            pos = str(t).lstrip('Span[').rstrip(']:').split(':')
            span['token_start']=  int(pos[0])  + token_count_offset
            span['token_end']= int(pos[1].rstrip(']')) -1 + token_count_offset   

            spans.append(span)
            


    # TODO DEBUG
    #print(f'\nspans:\n{spans}\n')

    return spans




def _get_token_dict(token_decoded, start, end, id, index):

    token_dict = {}
    token_dict['text'] = token_decoded
    # Start and end --> char level
    token_dict['start'] = start
    token_dict['end'] = end
    #token_dict['token_start'] = 
    #token_dict['token_end'] = 
    
    #TODO this id must be tokenizer id!!!!!!!!!!!!!!!!!!!!
    #DEBUG
    #token_dict['id'] = hash(token_decoded)
    #token_dict['id'] = hash(token_decoded)
    token_dict['id'] = index
    #token_dict['index'] = index


    return token_dict


def tokens_to_prodigy_dict(sentence , token_count_offset, token_char_offset):
    '''Returns a list of dicts (one dict per token)
        so that prodigy takes that (and not to make him to tokenize text)
    
        Example of a dict = {"text":"How","start":0,"end":3,"id":0}
    
    '''

    list_of_tokens = []
    #char_counter = 0
    for i,tok in enumerate(sentence):

        token_dict = _get_token_dict(tok.text, 
                                    #start = TODO,
                                    #end = TODO, 
                                    start = tok.start_position + token_char_offset ,
                                    end = tok.end_position + token_char_offset , 
                                    id=tok.idx + token_count_offset , # TODO RECHECK THIS
                                    index=tok.idx - 1 + token_count_offset, # TODO RECHECK THIS
                                    #index=tok.idx + token_count_offset,
                                    )
        #char_counter += len(token_decoded)

        list_of_tokens.append(token_dict)



    # TODO DEBUG
    #print(f'\list_of_tokens:\n{list_of_tokens}\n')

    return list_of_tokens




def annotate_this_pdf(tagger, txt_path, ntp_id):
    ''' Given the instantiated ner model, will open txt_path file and returns dictionary
    
    '''
    

    with open(txt_path, 'r') as f:
        doc_txt =  ''.join(f.readlines())
        #for l in f:
        #    file_txt_lines.append(l)



    doc_json = {}



    ##############################
    #           METHOD 1
    ##############################




    #doc_json['text'] = doc_txt
    #sentence = flair.data.Sentence(doc_txt)
    #tagger.predict(sentence)



    ##############################
    #           METHOD 2
    ##############################

    '''Splits text by phrase to not cause an OOM error!
    
    
    '''


    '''
    
    splitter = flair.splitter.SegtokSentenceSplitter()
    sentences = splitter.split(doc_txt)
    tagger.predict(sentences) # TODO: use batch here?


    doc_json['text'] = doc_txt
    #doc_json['text'] = ''
    doc_json['spans'] = []
    doc_json['tokens'] = []
    token_global_counter = 0
    last_sentence_char_end_position = 0

    #print('\n\n\nSentences----------------------------')
    #i_counter = 0

    for sentence in sentences:

        #TODO DEBUG
        #if(i_counter>=8):
        #    break
        #i_counter += 1 


        
        doc_json['spans'].extend( format_ner_spans( sentence, 
                                                    token_count_offset = token_global_counter, 
                                                    token_char_offset = last_sentence_char_end_position
                                                    ) 
                                )
        list_of_tokens_prodigy_format = tokens_to_prodigy_dict( sentence,
                                                                #token_count_offset = token_global_counter
                                                                token_count_offset = last_sentence_char_end_position
                                                                )                                                 
        doc_json['tokens'].extend(list_of_tokens_prodigy_format)

        #Update offsets
        token_global_counter += len(sentence)
        # plus one , so next sentence offset starts at 0
        last_sentence_char_end_position = sentence.end_position + 1 



        # DEBUGGGG
        #print('\n\n\n\n\n\n\n\n')
        #print('----')
        #print(sentence)
        #ppppansss = doc_json['spans'] 
        #print(f'sentence._start_position:{sentence._start_position}')
        #print(f'sentence.end_position:{sentence.end_position}')
        #print(f'\nSPANS UPDATED:\n{ppppansss}')
        #print(f'sentence.__dir__:{dir(sentence)}')

    

    '''

    ##############################
    #           METHOD 3
    ##############################

    '''SAme as method 2 but this only does not split by prashe it does by
        custom tag: split_tag = '#HEADING:'

        NOTE: FINAL BEHAVIOUR SEEMS TO     
    '''



    #doc_json['text'] = doc_txt

    
    split_tag = '#HEADING:\n'
    sentences = [split_tag+i for i in doc_txt.split(split_tag)]
    sentences[0] = sentences[0].replace(split_tag,'') 
    #doc_json['text'] = ''.join(sentences)
    #flair_sentences = [flair.data.Sentence(s, use_tokenizer=False) for s in sentences]

    #tagger.predict(sentences) # TODO: use batch here?

    doc_json['text'] = ''
    doc_json['spans'] = []
    doc_json['tokens'] = []
    token_global_counter = 0 # Counting the total number of tokens
    last_sentence_char_end_position = 0 # Counting the global char offset
    #print('\n\n\nSentences----------------------------')
    #for real_sentence, sentence in zip(sentences,flair_sentences):
    for  real_sentence in sentences:


        '''If  use_tokenizer=False line breaks will be preseverd, but somo entities will
            be bad.recognised (but is prtty much the same) '''
        #sentence = flair.data.Sentence(real_sentence, use_tokenizer=False)
        sentence = flair.data.Sentence(real_sentence, use_tokenizer=True)



        doc_json['text'] += sentence.text
        #assert len(real_sentence) == len(sentence.text), f'Las longitudes no coincide {len(real_sentence)} != {len(sentence.text)} \n real:\n{real_sentence} \
        #    \nsentence:\n{sentence.text}'

        tagger.predict(sentence)
        doc_json['spans'].extend( format_ner_spans( sentence, 
                                                    token_count_offset = token_global_counter, 
                                                    token_char_offset = last_sentence_char_end_position) 
                                )
        list_of_tokens_prodigy_format = tokens_to_prodigy_dict( sentence,
                                                                token_count_offset = token_global_counter,
                                                                token_char_offset = last_sentence_char_end_position
                                                                )                                              
        doc_json['tokens'].extend(list_of_tokens_prodigy_format)
        #doc_json['text'] += f'{sentence.text}\n'

        #Update offsets
        token_global_counter += len(sentence)
        # FLAIR OFFSET NOT WORKING WELL! WARNING:
        #last_sentence_char_end_position += sentence.end_position + 1 
        #last_sentence_char_end_position += sentence.end_position # NOT WORKING WELL! WARNING
        last_sentence_char_end_position += len(sentence.text)





        # TODO KNOW BUG (NOT IMPORTANT AT ALL):  doc_json['text']  does not contain final \n but the last token does for some reason (flair again)


    # DEBUGG DELETE TODO
    #with open('debug_output_splitting_manualmode.txt', 'w') as f:
    #    f.write(doc_json['text'])   

    


    # Additional metadata
    meta = {}
    meta['ntp_id'] = ntp_id
    meta['doc_name'] = os.path.basename(os.path.normpath(txt_path))
    doc_json['meta'] = meta


    return doc_json





def process_procurement(path, tagger):

    '''Recieves a ntpXXX folder path and yields dictorary of annotations (uses annotation per line:json)'''

    assert os.path.exists(path) , f'Error, cannot directory {path} does not exist!' 

    dir_list = os.listdir(path)


    last_nif_seen = None

    for i in dir_list:
        # i here is the report_relevant_pdf_list.....txt file!
        # Several nif check, it should never be the case, but just in case the gelpi api gave as more than one
        if PATTERN.match(i):

            # Several nif check,
            nif = i.rstrip('.txt').split('_nif_')[1]
            if(nif != last_nif_seen and last_nif_seen is not None ):
                print(f'WARNING: Several NIF were found for procurement under path: {path}')
            last_nif_seen = nif


            # Read the list of already-classified as "relevant" pdfs
            relevant_procurement_doc_names = []
            with open(os.path.join(path, i)) as fin:
                for l in fin:
                    relevant_procurement_doc_names.append( l.strip() )


            # Take each relevant doc
                    
            for doc in relevant_procurement_doc_names:

                print(f'\tProcessing docname: {doc}')

                # Name handling
                pdf_folder_name = doc.rstrip('.pdf')
                pdf_processed_subfolder_name = pdf_folder_name + '_processed_finished'
                pdf_complete_txt_path = os.path.join(path, pdf_folder_name, pdf_processed_subfolder_name, 'Doc_labeled_sections.txt')


                # calling the ner annotation function
                ntp_id = os.path.basename(os.path.normpath(path))
                annotated_doc_dict = annotate_this_pdf(tagger,  pdf_complete_txt_path, ntp_id)
                yield annotated_doc_dict



def get_organised_procurements_names(path):

    list_of_files = []
    os_logging.info(f'\t Asking OS to list dir:{path} ...')
    dir_list = os.listdir(path)
    os_logging.info(f'\t Done!...')
    return dir_list



def parse_args():

    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    



    parser.add_argument("--parentfolder",
                        default='<INTERNAL PATH>data_processed_organised',
                        help="Default parent folder where the organised ntpXXX folders are located"
                        )
    
    
    parser.add_argument("--device",
                        default=-1,
                        help="Device torch id (whether to use the GPU or not)"
                        )
    
    parser.add_argument("--output",
                        default='annotations_by_flairmodel.jsonl',
                        help="Where to save the annotation info. (e.g. output.jsonl)"
                        )
    
    

    #if len(sys.argv)==1:
    #    parser.print_help(sys.stderr)
    #    sys.exit(1)


    args = parser.parse_args()

    return args



def main(*args, **kwargs):

    args = parse_args(*args, **kwargs)

    assert args.output is not None, 'You must specify an output path!'

    parent_folder = args.parentfolder
    device = args.device

    print(f'\n\n\n>>>>Using data parent_folder:{parent_folder}\n')

    procurements_dirlist = get_organised_procurements_names(parent_folder)
    #print('procurements_dirlist')
    #print(procurements_dirlist)
    



    # Instantiate model
    tagger = flair.models.SequenceTagger.load(MODEL)
    print(f'USING MODEL:{MODEL} \n\n\n')
    print(f'>>> Annotations will be saved in:{args.output} \n\n\n')




    # Process each ntpXXX folder
    with open(args.output,'w') as fout:
        for p in procurements_dirlist:
            for doc_annotation in process_procurement( os.path.join( parent_folder, p) , tagger ):
                #print(doc_annotation)
                json.dump(doc_annotation, fout)
                fout.write('\n')
                fout.flush()


        #TODO DEBUG DELETE THIS
                #break
            #break


if __name__ == '__main__':
    main()


'''Innvocation example:
    python3 auto_ner_annotator.py --output annotations.jsonl
'''





