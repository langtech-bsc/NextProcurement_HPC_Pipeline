# Prodigy Recipe for flair NER Annotation task

Annotation script and prodigy recipe script have been developed in diferent files in order to improve computacional costs.


WARNING:python 10 or higher is required!


To install prodigy (>= 1.11.14) do (DO NOT SHARE OUR KEY!):
```python
 pip install prodigy -f https://6E41-2FD2-F54C-2276@download.prodi.gy 
```





To call the script do 




```python
 python3.10 -m prodigy nextprocurement_ner_manual_flair_with_bio <DATASET_GIVEN_NAME> <ANNOTATIONS_FILE>.jsonl -F nextprocurement_ner_manual_flair.py --label ORG,MISC,SINGLE_COMPANY
 ```


Open then http://localhost:8080 and a UI like this will appear:
![alt text](https://github.com/langtech-bsc/NextProcurement_Complete_Pipeline/blob/main/annotation/prodigy/recipes/.img/prodigy_examples.png "Annotation example")



To save annotations click on save button at the top left corner. Then execute this command:
```python
 prodigy db-out  <DATASET_GIVEN_NAME> >  <filename>.jsonl
```

to export the annotations to a jsonl file. Remember to use the same dataset name you used in the previous commands!



ENJOY your annotattion task!
