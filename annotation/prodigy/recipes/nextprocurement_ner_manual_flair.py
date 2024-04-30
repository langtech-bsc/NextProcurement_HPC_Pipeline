from typing import List, Optional
import spacy

import prodigy
from prodigy.components.loaders import JSONL
from prodigy.components.preprocess import add_tokens
from prodigy.models.matcher import PatternMatcher
from prodigy.util import split_string
from prodigy.components.loaders import get_stream

import sys


# Helper function for removing token information from examples
# before they're placed in the database. Used if character highlighting is enabled.
# It is called when save button is pressed in the UI.
def remove_tokens(answers):

    #print('remove_tokens just called!')
    #sys.stdout.flush()

    #for eg in answers:

        #del eg["tokens"]
        #if "spans" in eg:
        #    for span in eg["spans"]:
        #        del span["token_start"]
        #        del span["token_end"]
    return answers



def make_tasks(nlp, stream, labels):
    """Add a 'spans' key to each example, with predicted entities."""
    # Process the stream using spaCy's nlp.pipe, which yields doc objects.
    # If as_tuples=True is set, you can pass in (text, context) tuples.



    print('make_tasks just called!')
    sys.stdout.flush()

    texts = ((eg["text"], eg) for eg in stream)
    for doc, eg in nlp.pipe(texts, as_tuples=True):
        task = copy.deepcopy(eg)
        spans = []
        for ent in doc.ents:
            # Ignore if the predicted entity is not in the selected labels.
            if labels and ent.label_ not in labels:
                continue


            spans.append(
                {
                    "token_start": ent.token_start,
                    #"token_end": ent.token_end - 1 ,
                    "token_end": ent.end,
                    "start": ent.start,
                    "end": ent.end,
                    "text": ent.text,
                    "label": ent.label_,
                }

            
            )
        task["spans"] = spans
        # Rehash the newly created task so that hashes reflect added data.
        task = set_hashes(task)
        yield task




# Recipe decorator with argument annotations: (description, argument type,
# shortcut, type / converter function called on value before it's passed to
# the function). Descriptions are also shown when typing --help.
@prodigy.recipe(
    "nextprocurement_ner_manual_flair_with_bio",
    dataset=("The dataset to use", "positional", None, str),
    source=("The source data as a JSONL file", "positional", None, str),
    label=("One or more comma-separated labels", "option", "l", split_string),
    patterns=("The match patterns file","option","p",str),
    exclude=("Names of datasets to exclude", "option", "e", split_string),
    highlight_chars=("Allow for highlighting individual characters instead of tokens", "flag", "C", bool),
    loader=("Loader (guessed from file extension if not set)", "option", "lo", str),

)
def ner_manual(
    dataset: str,
    #spacy_model: str,
    source: str,
    label: Optional[List[str]] = None,
    patterns: Optional[str] = None,
    exclude: Optional[List[str]] = None,
    highlight_chars: bool = False,
    loader: Optional[str] = None,

):
    """
    Mark spans manually by token. Requires only a tokenizer and no entity
    recognizer, and doesn't do any active learning. If patterns are provided,
    their matches are highlighted in the example, if available. The patterns file can
    include exact strings or token patterns for use with spaCy's `Matcher`.
    The recipe will present all examples in order, so even examples without matches are shown.
    If character highlighting is enabled, no "tokens" are saved to the database.
    """

    special_tokens = ()

    stream = get_stream(source, loader=loader, input_key="text")



    def add_tokens(stream):

        #DEBUG
        print('add_tokens just called!')
        sys.stdout.flush()


        for eg in stream: # eg is each task

            task = {}
            task['text'] = eg["text"]

            eg_tokens = []
            idx = 0

            tokens = eg['tokens']
            for token in tokens:

                token_prodigy_dict = {
                    "text": token['text'],
                    "start": token['start'],
                    "end": token['end'],
                    #"token_start": token['token_start'],
                    #"tokend": token['token_end'],

                    #"tokenizer_id": token['id'],
                    #"id": token['id'],

                    # Don't allow selecting spacial SEP/CLS tokens
                    #"disabled": token['text'] in special_tokens,
                }
                eg_tokens.append(token)
                idx += 1



            for i, token in enumerate(eg_tokens):
                # If the next start offset != the current end offset, we
                # assume there's whitespace in between
                if i < len(eg_tokens) - 1 and token["text"] not in special_tokens:
                    next_token = eg_tokens[i + 1]
                    token["ws"] = (
                        next_token["start"] > token["end"]
                        or next_token["text"] in special_tokens
                    )
                else:
                    token["ws"] = True
            task["tokens"] = eg_tokens


            # Spans
            assert 'spans' in eg, 'ERROR Could not find spans in eg'
            if('spans' in eg ):
               task['spans'] = eg['spans']
  

            
            yield task


    # If `use_chars` is True, tokens are split into individual characters, which enables
    # character based selection as opposed to default token based selection.
    stream = add_tokens(stream)



    return {
        "view_id": "ner_manual",  # Annotation interface to use
        "dataset": dataset,  # Name of dataset to save annotations
        "stream": stream,  # Incoming stream of examples
        "exclude": exclude,  # List of dataset names to exclude
        #"before_db": remove_tokens if highlight_chars else None,
        "before_db": remove_tokens,
        # Remove token information to permit highlighting individual characters
        "config": {  # Additional config settings, mostly for app UI
            "labels": ["ORG", "MISC", "SINGLE_COMPANY"],  # Selectable label options
            "hide_true_newline_tokens" : False,
            "custom_theme": {
                "labels": {
                "ORG": "#00FF00",
                "MISC": "#FFFF00",
                "SINGLE_COMPANY" : "#ADD8E6",
            },
        },
        },
        
    }



''' RUNNING FORMAT 

    RUN
        python3.10 -m prodigy <RECIPE_NAME>  <DATASET_GIVEN_NAME> <annotations_file>.jsonl \ 
            -F ./recipes/<recipes_file>.py --label <COMMA_SEPARATED_LABELS>

        
    Remove previous datset if already created with the same name:
        
        python3.10 -m prodigy drop <DATASET_GIVEN_NAME>

    Export annotations with:

        python3.10 -m prodigy db-out  <DATASET_GIVEN_NAME> >  <filename>.jsonl
    
    
'''




''' EXAMPLE

    REMOVE SAME-DATASET NAME FROM PREVIOUS EXECUTIONS:
        
        python3.10 -m prodigy drop test_dataset


    RUN

        python3.10 -m prodigy nextprocurement_ner_manual_flair test_dataset annotations/annotations_by_flairmodel.jsonl -F ./recipes/nextprocurement_ner_manual_flair.py --label ORG,MISC,SINGLE_COMPANY


    EXPORT
        
        prodigy db-out test_dataset >  corrected_annotations/corrected_test_dataset.jsonl

'''



