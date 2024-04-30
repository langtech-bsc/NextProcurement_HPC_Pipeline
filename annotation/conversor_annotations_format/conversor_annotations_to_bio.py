''' This script takes prodigy corrections (annotations.jsonl) file for a NER tasks
    and coverts it to a txt following the BIO format.

    CONLL (IOB2 or BIO) format


    WARNING: It will only take "accepted" prodigy answers

'''

import argparse
import json
import copy
import os
import sys


DOCS_SEPARATOR_TAG = '-DOCSTART-'


'''
# This just does not work --> vocab needed, among other problems
#Using Spacy Library

#import spacy
#from spacy.training import offsets_to_biluo_tags

def convert_doc(doc):

    nlp = spacy.blank("es") # TODO CHECK LANG if it matters (it shouldnt)
    document = nlp(doc['text'])
    print(f'debug document[3:7]{document[3:7]}')
    
    offsets = [ (span["start"], span["end"], span["label"]) for span in doc["spans"] ]
    biluo_tags = offsets_to_biluo_tags(document, offsets)
    #print(f'biluo_tags{ biluo_tags}')

    document.ents = [document.char_span(start, end, label) for start, end, label in offsets]
    print(f'document.ents{ document.ents}')
    iob_tags = [f"{t.ent_iob_}-{t.ent_type_}" if t.ent_iob_ else "O" for t in document]
'''



def _get_next_span_info(spans_iterable):
    current_span = copy.deepcopy(next(spans_iterable))
    #current_span_start = current_span['start']
    #currennt_span_end = current_span['end']
    current_span['n_of_tokens_left'] = int(current_span['token_end']) - int(current_span['token_start'])
    return current_span



def _convert_doc_no_spans(doc):
    '''If doc has no spans, we make the docs processing quicker
    '''

    doc_in_conll_format = []

    for token in doc['tokens']:
        # O-TAG
        token_text = token['text']
        current_token_conll_tag = f'{token_text}\tO'

        # OUTPUT TOKEN WRTTING Without whitespace handling (seems its better this way)
        doc_in_conll_format.append (current_token_conll_tag)


    return doc_in_conll_format





def convert_doc(doc):
    '''Manually processing prodigy output to CoNLL 2003 format'''


    if(len(doc['spans']) == 0): # no spans
        return _convert_doc_no_spans(doc)
    

    # Spans consumer interable
    doc_in_conll_format = []
    spans_iterable = iter(doc['spans'])
    current_span = _get_next_span_info(spans_iterable)

    #  Flow control variables
    _update_span_flag = False
    _b_span_tag_opened = False

    # Matching between tokens and spans
    for token in doc['tokens']:
        current_token_conll_tag = ''
        
        if(int(token['start']) ==  int(current_span['start']) ): # B -TAG
            token_text = token['text']
            label = current_span['label']

            if(current_span['n_of_tokens_left'] == 0): 
                # Single Token Tag
                _update_span_flag = True
                _b_span_tag_opened = False

                current_token_conll_tag = f'{token_text}\tI-{label}'
                # debug assert(it can be commented in the future for efficiency reasons)
                if('text' in current_span):
                    assert current_span['text'] == token['text'], f'Error current span start text does not match current token text (span formed by a single word)'

            else:
                #Multi token tag
                _b_span_tag_opened = True

                current_token_conll_tag = f'{token_text}\tB-{label}'
                current_span['n_of_tokens_left'] = current_span['n_of_tokens_left'] - 1 

        elif( _b_span_tag_opened  and current_span['n_of_tokens_left'] > 0 ): # I-TAG (not the last I tag)
            
            token_text = token['text']
            label = current_span['label']
            current_token_conll_tag = f'{token_text}\tI-{label}'
            current_span['n_of_tokens_left'] = current_span['n_of_tokens_left'] - 1 

            # debug assert (remove in the future for efficiency reasons)
            assert current_span['n_of_tokens_left'] >= 0 , 'Error with intermediate span handling!!'

        elif(_b_span_tag_opened and current_span['n_of_tokens_left'] == 0 ): # I-TAG (last)

            assert token['end'] ==  current_span['end'] , f'Error current span end  does not match current token end'
            
            token_text = token['text']
            #print('token_text{token_text}')
            #print(f'current_span:{current_span}')
            label = current_span['label']
            current_token_conll_tag = f'{token_text}\tI-{label}'
            _update_span_flag = True
            _b_span_tag_opened = False

        else: # O-TAG
            token_text = token['text']
            current_token_conll_tag = f'{token_text}\tO'
            _b_span_tag_opened = False



        # OUTPUT TOKEN WRITING with with  Whitespaces handling
        '''
        if( token['ws'] ): 
            #if(current_span['n_of_tokens_left'] > 0): # if we are still inside the NER entity put the text inside the NER text
            if(_b_span_tag_opened): 
                # if we are still inside the NER entity put the text inside the NER text
                current_token_conll_tag += ' ' # fix this (it puts the space ant the end of the TAG!)
                doc_in_conll_format.append (current_token_conll_tag)
            else:
                doc_in_conll_format.append (current_token_conll_tag)
                doc_in_conll_format.append (f' \tO')
        else:
            doc_in_conll_format.append (current_token_conll_tag)

        '''

        # OUTPUT TOKEN WRTTING Without whitespace handling (seems its better this way)
        doc_in_conll_format.append (current_token_conll_tag)





        if(_update_span_flag):
            try:
                current_span = _get_next_span_info(spans_iterable)
                _update_span_flag = False
            except StopIteration:
                # No more spans available in the doc
                pass


    # DEBUG: FINAL ASSERTS
    # Check if there are elements left
    try:
        #next_item = next(spans_iterable)
        next_item = _get_next_span_info(spans_iterable)
    except StopIteration:
        #print("Iterator is empty")
        pass
    else:
        raise Exception(f"ERROR: There are still spans in the Iterator. Iterator next item:{next_item}")


    return doc_in_conll_format






def parse_args():

    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    


    parser.add_argument("--input",
                        default=None,
                        help="Prodigy input annotations.jsonl file"
                        )
    
    
    parser.add_argument("--output",
                        default='annotations_converted.txt',
                        help="Path where to save the output .txt file"
                        )
    
    parser.add_argument("--remove_mark_between_docs",
                        action='store_true',
                        default=False,
                        help="""If passed, will put a '-DOCSTART-' string between diferrent documents."""
                        )
    

    args = parser.parse_args()

    return args





def main(*args, **kwargs):
    
    args = parse_args(*args, **kwargs)
    assert args.input is not None, 'You must specify an input file!'


    #stats 
    n_total_docs = 0 
    n_accepted = 0

    with open(args.input, 'r') as f:
        with open(args.output, 'w') as fout:
            for l in f :
                corrected_doc = json.loads(l)
                n_total_docs += 1

                if(corrected_doc['answer'] == 'accept'):
                    n_accepted += 1

                    if(not args.remove_mark_between_docs):
                        fout.write(f'{DOCS_SEPARATOR_TAG}\n')
                    doc_in_conll_format = convert_doc(corrected_doc)
                    fout.write('\n'.join(doc_in_conll_format))
                    fout.write('\n')




    print(f'Conversion completed and saved in {args.output}!\
          \n\t STATS: {n_accepted} accepted documented processed from a total of {n_total_docs}\
            docs contained. \n\t Input file: {args.input}! ')           


if __name__ == '__main__':
    main()


''' Run example

    python conversor_annotations_to_bio.py --input ./test_data/input_corrected_1_annotation.jsonl
'''