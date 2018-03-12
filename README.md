# paraBTM
## System Overview
A parallel biomedical Named Entity Rrcognition framework on the Tianhe-2 supercomputer. We integrate several bio-NER tools (TmVar, GNormPlus, and DNorm) and develop a data-level parallel processing framework. The framework unifies the input-output streams and can recognize genes, mutations and diseases appearing in biomedical literatures. Besides, this framework optimized the load balancing strategy to achieve a better efficiency of parallel processing and other tasks of biomedical text mining can be readily integrated into the framework.  
## System environment requirements  
## Installation Instruction 
See readme.txt in 
## Command line instructions  

### Example:
yhbatch -n 64 -N 15 run_pmc_detectors_0125_eff.sh 64 15 ./temp/mutation_pmids.txt pubmed PMC2PMID.txt pmc_nxml_list.txt gnorm-out tmvar-out tmvar
### Option Description:
"	-n -- number of processes  
"	-N -- number of nodes  

### Argument Description:
$1= number of processes   
$2= number of nodes   
$3= "ids"--Specify the file containing article ids of interest  
$4= "idtype"--Specify the type of ids  
$5= "idmap"--Specify the mapping from PMID to PMCID   
$6= "pmc"--Specify the file with PMC XML paths   
$7= "input"--Specify the input folder   
$8= "output"--Specify the output folder  
$9= "detector"--Specify the detector   

