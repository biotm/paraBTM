# paraBTM
## System Overview
A parallel biomedical Named Entity Rrcognition framework on the Tianhe-2 supercomputer. We integrate several bio-NER tools (TmVar, GNormPlus, and DNorm) and develop a data-level parallel processing framework. The framework unifies the input-output streams and can recognize genes, mutations and diseases appearing in biomedical literatures. Besides, this framework optimized the load balancing strategy to achieve a better efficiency of parallel processing and other tasks of biomedical text mining can be readily integrated into the framework.  
## System environment requirements 
Configuration related to text mining in Tianhe-2 parallel computing system is complicated, if you want to try paraBTM in Tianhe-2, please be free to contact me:istarxytt@gmail.com.
## Installation Instruction 
Download biomedical NER tools tmVarJava, GNormPlusJava and DNorm-0.07 and install them according to the readme files.
## Command line instructions  

### Example:
yhbatch -n 64 -N 15 run_pmc_detectors.sh 64 15 $1 $2 $3 $4 $5 $6 $7
### Option Description:
"	-n -- number of processes  
"	-N -- number of nodes  

### Argument Description:
$1= "ids"--Specify the file containing article ids of interest.  
$2= "idtype"--Specify the type of ids("pubmed" or "pmc").  
$3= "idmap"--Specify the mapping from PMID to PMCID  
$4= "pmc"--Specify the file with PMC XML paths  
$5= "input"--Specify the input folder  
$6= "output"--Specify the output folder  
$7="detector"--Specify the detector(Random/Shortest-Bucket/Round-Robin/tmVar/dnorm/Gnorm/), it determines the loading strategies to use or plugins to call.  
 
## Example of related files:
1.	Input File in PubTator Format:
	<PMID>|t|<TITLE>
	<PMID>|a|<ABSTRACT>
	<PMID><tab><OFFSET_START><tab><OFFSET_END><tab><Gene mention><tab>Gene<tab><Gene ID>
2.	Pmc ids:

3.	Pmc nxml paths:
 
4.	Pmc2pmcid file:
 
## Result and analysis 

Experimental results validate that paraBTM effectively improve the processing speed of biomedical named entity recognition.


 

Figure 1 The time cost of processing different input sizes in serial

 
Figure 2 Effects of different load balancing strategies.


Figure 2 shows the time spent on paraBTM processing with different numbers of parallel processes on an input dataset of 16 MBs (including 175 articles) which is composed of articles randomly selected from the 60K corpus. Figure 3 shows the loading strategy efficiencies.
 
Figure 3 Load balancing efficiencies


