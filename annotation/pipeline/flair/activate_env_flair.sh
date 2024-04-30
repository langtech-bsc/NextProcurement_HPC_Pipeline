#!/bin/bash

module load gcc python/3.9.1

source env_flair/bin/activate

export PYTHONPATH=<INTERNAL BSC PATH>NER_test/env_flair/lib/python3.9/site-packages:$PYTHONPATH