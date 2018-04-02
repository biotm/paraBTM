import glob
import os
import sys
import pubmed_parser as pp
import redis
from mpi4py import MPI
import shutil
import unicodedata
import re
import argparse
import unicode2ascii.unicode2ascii as U2A
import subprocess
import os.path
import math
import shutil
import time



def get_FileSize(filePath):
    filePath = unicode(filePath,'utf8')
    fsize = os.path.getsize(filePath)
    fsize = fsize/float(1024*1024)
    return round(fsize,2)

def sort(filePath):
    #fileList = []
    fileMap = {}
    #size = 0
    for parent, dirnames, filenames in os.walk(filePath):
        for filename in filenames:
            size = os.path.getsize(os.path.join(parent, filename))
            fileMap.setdefault(os.path.join(parent, filename), size)
    k = sorted(fileMap.items(), key=lambda d: d[1], reverse=True)
    return k


def rb(list,processnum):
    processlist = []
    for i in range(processnum):
        subtasklist = []
        process = [i, 0, subtasklist]
        processlist.append(process)
    i=0
    for task in list:
        ind=(i)%len(processlist)
        if (i/len(processlist))%2==0:
            processlist[ind][1]+=task[1]
            processlist[ind][2].append(task[0])
        else:
            processlist[len(processlist)-1-ind][1]+=task[1]
            processlist[len(processlist)-1-ind][2].append(task[0])

        i+=1
    return processlist


def choosemin(task,proclist):
    proclist.sort(key=lambda x: x[1])
    proclist[0][1]+=task[1]
    proclist[0][2].append(task[0])
    return proclist[0],proclist


def bucket(list,processnum):
    processlist=[]
    for i in range(processnum):
        subtasklist=[]
        process=[i,0,subtasklist]
        processlist.append(process)
    for task in list:
        activeprocess,processlist=choosemin(task,processlist)
    return processlist


def findname(s):
    l=s.split('/')
    return l[-1]

#def generate_rank_load(processlist):
 #   workload = {}
  #  for index, capacity, sublist in processlist:
   #     if index in workload:
    #        workload[index].addall(sublist)
     #   else:
      #      workload[index] = set()

    #return workload

def generate_directory_rb(processlist,dir):
    for index, capacity, sublist in processlist:
        if os.path.isdir(dir+'/' + str(index)):
            pass
        else:
            os.makedirs(dir+'/' + str(index))
        for f in sublist:
            shutil.copy(f, dir+'/' + str(index) + '/' + findname(f))

def generate_directory_bucket(processlist,dir):
    for index, capacity, sublist in processlist:
        if os.path.isdir(dir+'/' + str(index)):
            pass
        else:
           # if (rank == 0):
            os.makedirs(dir+'/' + str(index))
        for f in sublist:
            shutil.copy(f, dir+'/' + str(index) + '/' + findname(f))




def shedule_roundrobin(input_dir,output_dir,num):
    fl = sort(input_dir)
    finl = rb(fl, num)
    generate_directory_rb(finl, output_dir)


def shedule_shortestbucket(input_dir,output_dir,num):
    fl = sort(input_dir)
    finl = bucket(fl, num)
    generate_directory_bucket(finl, output_dir)

#def random()


# Path = '/Users/kerr/PycharmProjects/run_mpi_pmc_command/file_collection'
# directory = '123321'
#
# b = sort(Path)
# l=bucket(b,3)
# generate_directory_bucket(l,directory)
# p=rb(b,3)
# generate_directory_rb(p,directory)





def u2a_convert(id, in_str, tmp_suffix):
    if len(in_str) == 0:
        return ''

    ftmp_name = '/tmp/%s.%s' % (id, tmp_suffix)
    if isinstance(in_str, unicode):
        in_str = unicodedata.normalize('NFKD', in_str).encode('ascii', 'ignore')

    ftmp = open(ftmp_name, 'w')
    ftmp.write(in_str)
    ftmp.close()
    U2A.processFile(ftmp_name)
    ftmp_read = open(ftmp_name, 'r')
    new_str = ftmp_read.readlines()[0]
    new_str = re.sub(r'\s\s+', ' ', new_str)

    return new_str

def pmc2txt(xml_in, pmcid, job_size, dest_dir):
    try:
        pubmed_out = pp.parse_pubmed_xml(xml_in)
        ft_out = pp.parse_pubmed_paragraph(xml_in, all_paragraph=False)
    except Error as e:
        print 'Error in parsing nxml file %s' % xml_in
        return

    cnt = 0
    bcnt = 0

    #print 'PMC2Txt', xml_in

    pmcid_no = pmcid.replace('PMC', '')
    sub_dir = '%s/%d' % (dest_dir, int(pmcid_no) % job_size)

    full_text = ''

    for paragraph in ft_out:
        if 'text' in paragraph:
            full_text += paragraph['text']

    full_text = u2a_convert(pmcid, full_text, 'fulltext')

    if not os.path.exists(sub_dir):
        os.makedirs(sub_dir)

    f_tmp_in_fn = '%s/%s.txt' % (sub_dir, pmcid)
    f_tmp_in = open(f_tmp_in_fn, 'w')

    f_tmp_in.write(full_text)
    f_tmp_in.close()


def get_pmids(pmids_in):
    pmids = {}
    f = open(pmids_in, 'r')
    for pmid in f.readlines():
        pmid = pmid.strip()
        pmids[pmid] = 1
    f.close()
    return pmids

def get_pmcids(pmcids_in):
    pmcids = {}
    f = open(pmcids_in, 'r')
    for pmcid in f.readlines():
        pmcid = pmcid.strip()
        pmcids[pmcid] = 1
    f.close()
    return pmcids

def get_ID_mappings(mapping_fn, pmids):
    mapping_file = open(mapping_fn, 'r')

    pmid2pmcid = {}
    pmcid2pmid = {}

    #print 'Getting ID mappings'

    for line in mapping_file.readlines():
        line = line.strip()
        (pmcid, pmid) = line.split(',')
        pmcid = pmcid.strip()
        pmid = pmid.strip()

        if pmid in pmids:
            pmid2pmcid[pmid] = pmcid
            #print '%s -> %s' % (pmid, pmcid)

    mapping_file.close()
    return pmid2pmcid

def getpmcid2path(pmc_path_fn, pmcids):
    pmcid2path = {}

    pmc_path_file = open(pmc_path_fn, 'r')

    for pmc_path in pmc_path_file.readlines():
        pmc_path = pmc_path.strip()
        pmcid = os.path.basename(pmc_path).replace('.nxml', '')

        if pmcid in pmcids:
            pmcid2path[pmcid] = pmc_path

    pmc_path_file.close()

    return pmcid2path


def pmc2pubtator(xml_in, pmcid, job_size, dest_dir):
    try:
        pubmed_out = pp.parse_pubmed_xml(xml_in)
        ft_out = pp.parse_pubmed_paragraph(xml_in, all_paragraph=False)
    except Error as e:
        print 'Error in parsing nxml file %s ' % xml_in
        return -1

    cnt = 0
    bcnt = 0

    #print 'PMC2Txt', xml_in

    pmcid_no = pmcid.replace('PMC', '')

    full_text = ''

    for paragraph in ft_out:
        if 'text' in paragraph:
            full_text += paragraph['text']

    full_text = u2a_convert(pmcid, full_text, 'fulltext')

    pmcnumber = pubmed_out['pmc']
    
    title = pubmed_out['full_title']
    abstract = pubmed_out['abstract']
    ttle= u2a_convert(pmcid, title, 'title')
    abst= u2a_convert(pmcid, abstract, 'abstract')


    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    f_tmp_in_fn = '%s/%s.txt' % (dest_dir, pmcid)
    if os.path.exists(f_tmp_in_fn):
	with open(f_tmp_in_fn) as f_tmp_in:
		if len(f_tmp_in.readlines()) > 0:
			f_tmp_in.close()
			return 1	

    f_tmp_in = open(f_tmp_in_fn, 'w')

    f_tmp_in.write(pmcnumber+'|t|'+ttle.strip())
    f_tmp_in.write('\n')
    f_tmp_in.write(pmcnumber+'|a|'+abst.strip()+full_text.strip())
    f_tmp_in.write('\n')
    f_tmp_in.write('\n')

    f_tmp_in.close()
    return 1

if __name__ == "__main__":
    # === Get Options
    arg_parser = argparse.ArgumentParser()
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("ids", help="Specify the file containing article ids of interest.")
    arg_parser.add_argument("idtype", help="Specify the type of ids.")
    arg_parser.add_argument("idmap", help="Specify the mapping from PMID to PMCID")
    arg_parser.add_argument("pmc", help="Specify the file with PMC XML paths")
    arg_parser.add_argument("input", help="Specify the input folder")
    arg_parser.add_argument("output", help="Specify the output folder")
    arg_parser.add_argument("detector", help="Specify the detector")
   # arg_parser.add_argument("dest_dir", help="Specify the directory to store generated TXT files.")
    args = arg_parser.parse_args()
    ids_fn = args.ids
    inputtype = args.idtype
    id_mappings_fn = args.idmap
    pmc_nxml_list_fn = args.pmc
    input_folder = args.input
    output_folder = args.output
    detector = args.detector
    valid_size=0

    #Getting absolute paths
    cwd = os.getcwd()
    input_folder = os.path.abspath(input_folder)
    output_folder = os.path.abspath(output_folder)
    
    # =================================================
    # MPI initialization
    #inistart = MPI.Wtime()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    name = MPI.Get_processor_name()
    #elapsed = 0
    #Input
    if not os.path.exists(input_folder):
        print "Input folder: %s does not exist" % input_folder
        sys.exit()
   # else:
       # sub_dir = '%s/%d' % (input_folder, rank)
       # if not os.path.exists(sub_dir):
           # os.makedirs(sub_dir)

    #Output
    #if rank==0:
        #if not os.path.exists(output_folder):
        #    os.makedirs(output_folder)
    
       # sub_dir = '%s/%d' % (output_folder, rank)
      #  if not os.path.exists(sub_dir):
     #       os.makedirs(sub_dir)
    #comm.Barrier()
    if rank == 0:
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        for i in range(0, size):
            sub_dir = '%s/%d' % (output_folder, i)
            if not os.path.exists(sub_dir):
                os.makedirs(sub_dir)
    comm.Barrier()
    #Dealing with input PMIDs or PMCIDs
    pmcid2path ={}
    valid_pmcids = []

    pmids = get_pmids(ids_fn)
    if (inputtype == 'pubmed'):
        pmid2pmcid = get_ID_mappings(id_mappings_fn, pmids)
        pmcids = {key: 1 for key in pmid2pmcid.values()}
        pmcid2path = getpmcid2path(pmc_nxml_list_fn, pmcids)

        cnt = 0
        for pmid in pmids.keys():

            if pmid not in pmid2pmcid:
		#valid_pmcids.append('erro1')
                continue

            pmcid = pmid2pmcid[pmid]

            if pmcid not in pmcid2path:
		#valid_pmcids.append('erro2')
                continue
            address=pmcid2path[pmid2pmcid[pmid]].strip()

            valid_size+=get_FileSize(address)
            if valid_size<=16:
	    	valid_pmcids.append(pmid2pmcid[pmid])

        print '%d PMIDs can be mapped to PMC IDs' % len(valid_pmcids)

    if (inputtype == 'pmc'):
        pmcids = get_pmcids(ids_fn)
        pmcid2path = getpmcid2path(pmc_nxml_list_fn, pmcids)

        for pmcid in pmcids:
            if pmcid not in pmcid2path:
                continue
            valid_pmcids.append(pmcid)

        print '%d PMCIDs paths found.' % len(valid_pmcids)
    #inispent = MPI.Wtime()-inistart
    #Running detectors
    if detector == 'Random':
	#count=count+1
	start = MPI.Wtime()
        print 'Converting PMC XMLs to txt files in PubTator format on rank %d' % rank
       # print 'size:%d '%(size)
        #print 'rank:%d '%(rank)
	print valid_pmcids 
        for i in range(0, len(valid_pmcids)):
            pmcid = valid_pmcids[i]
            xml_in = pmcid2path[pmcid].strip()
	    print 'this is on rank %d,and i:%d,size:%d' % (rank,i,size)
            if i % size == rank:
                print 'Processing %s on rank %d' % (xml_in, rank)
                sub_dir = '%s/%d' % (output_folder, rank)
                pmc2pubtator(xml_in, pmcid, size, sub_dir)

        print 'PubTator files written by rank %d' % rank
        os.chdir(cwd)

    if (detector == 'Round-Robin'):
        start = MPI.Wtime()
        shedule_roundrobin(input_folder,output_folder, size)
        

    if (detector == 'Shortest-Bucket'):
        start = MPI.Wtime()
        shedule_shortestbucket(input_folder, output_folder, size)
        

    if detector == 'gnorm':
        start = MPI.Wtime()
        #start = time.clock()
        print 'Going to run GNorm on rank %d' % rank
        #startg = time.clock()
        os.chdir('/WORK/pp216/GNormPlus')
        os.system('perl GNormPlus.pl -i %s/%d -o %s/%d setup.txt' % (input_folder, rank, output_folder, rank))
        os.system('rm %s/%d/*.xml' % (output_folder, rank))
        os.chdir(cwd)
        print 'GNorm finished by rank %d' % rank
        #elapsedg = (time.clock() - startg)
	
        # print("Time used for gnorm:", elapsedg)
        #if (elapsedg > global_elapsed):
	 #   global_elapsed = elapsedg
	

    if detector == 'tmvar':
        start = MPI.Wtime()
        #start = time.clock()
        print 'Going to run tmVar on rank %d' % rank
        #startt = time.clock()
        os.chdir('/WORK/pp216/ParaTM/XYT/tmVar/tmVarJava')
        os.system('java -Xmx5G -Xms5G -jar tmVar.jar %s/%d %s/%d' % (input_folder, rank,output_folder, rank))
        print 'tmVar finished by rank %d' % rank
        os.chdir(cwd)
        #elapsedt = (time.clock() - startt)
        #print("Time used for tmvar:", elapsedt)
	
        #if (elapsedt >global_elapsed):
         #   global_elapsed = elapsedt
        #print ("Time used for tmvar:", elapsedt)

    if detector == 'dnorm':
        start = MPI.Wtime()
        #start = time.clock()
        print 'Going to run DNorm on rank %d' % rank
        #startd = time.clock()
        if os.path.exists('%s/%d' % (input_folder, rank)):
            os.system('find %s/%d -name "*.txt" ! -name dnorm.txt -exec cat {} + > %s/%d/dnorm.txt' % (input_folder, rank, input_folder, rank))

            os.chdir('/WORK/pp216/DNorm-0.0.7')
            os.system('./ApplyDNorm.sh config/banner_NCBIDisease_TEST.xml data/CTD_diseases.tsv.old output/simmatrix_NCBIDisease_e4.bin /WORK/pp216/Ab3P-v1.5 /tmp %s/%d/dnorm.txt %s/dnorm_out-%d.txt' % (
                input_folder, rank, output_folder, rank))
            os.chdir(cwd)
           # elapsedd = (time.clock() - startd)
            #print("Time used for tmvar:", elapsedd)
       # if (elapsedd > global_elapsed):
        #    global_elapsed = elapsedd
        #print ("Time used for dnorm:", elapsedd)

    #print count
    print 'Process %d finished successfully.' % rank
    #print 'Time used iis:%d '%(global_elapsed)
    #comm.Barrier(
    #elapsed = (time.clock() - start)
    #print("Time used:", elapsed)
    #comm.Barrier()
    #elapsed = ( MPI.Wtime() -  start)+inispent
    elapsed =  MPI.Wtime() -  start
    print ("Time used:",elapsed)
    timelist = []
    totaltime = 0
    maxtime = 0
    avgtime = 0
    #comm.Barrier()
    if rank > 0:
        comm.send(elapsed, dest=0)
	#comm.Barrier()
    if rank == 0:
        timelist.append(elapsed)
        for i in range(1,size):
	    timelist.append(comm.recv(source = i))
        timelist.sort(reverse = True)
	for j in range (len(timelist)):
		totaltime = totaltime + timelist[j]
	maxtime = max(timelist)
	avgtime = totaltime/len(timelist)
        #print ("Time used:",elapsed)
	print ("the max time is:",maxtime)
	print ("the avg time is:",avgtime)
	print ("the eff is:",avgtime/maxtime)
	print ("num of timelist: ",len(timelist))
