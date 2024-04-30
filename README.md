# NextProcurement_Complete_Pipeline
Complete NextProcurement (Internal) Pipeline. pdf2txt, NER extractor and more!

WARNING: DO NOT SHARE since it contains sensitive internal info (internal path and usernames)!


![alt text](https://github.com/langtech-bsc/NextProcurement_Complete_Pipeline/blob/main/.img/nextprocurement_logo.jpeg "NextProcurementLogo")




This repo will be updated while the subpipelines are being connected to keep it ordered!



The overall* workflow can be see in the following diagram:
(*NOTE: Some part are not including for simplification purposes)


![alt text](https://github.com/langtech-bsc/NextProcurement_Complete_Pipeline/blob/main/workflow/NextProcurement_Workflow.png "NextProcurementWorkflow_as_png")



NOTE: Remember to download AI model for pdf2txt (not included due to its large size):

```python
cd pipeline/pdf2txt/model_sections_classifier
git lfs install
git clone https://huggingface.co/BSC-LT/NextProcurement_pdfutils
 ```
