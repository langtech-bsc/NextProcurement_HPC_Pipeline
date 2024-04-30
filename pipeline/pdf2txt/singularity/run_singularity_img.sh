cd <INTERNAL GPFS PATH>NextProcurement/pipeline/pdf2txt/singularity
singularity exec pdf2txt.sif bash


#pdf2txt test
cd <INTERNAL GPFS PATH>NextProcurement/pipeline/pdf2txt/only_txt_generator_version/pdf2txt
python3 pipeline/pipeline.py -i examples_input_folder/ntp00000014_Pliego_Prescripciones_tecnicas_URI.pdf --output test_folder -m <INTERNAL GPFS PATH>NextProcurement/pipeline/pdf2txt/model_sections_classifier 