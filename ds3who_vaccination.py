"""
    Client : WHO - Nature Labs
    Project Scope : GCP as Infrastructure Modernization 
    Functional scope : Function to create 'Use Cases' in Google Cloud Platform
    GCP Projects Used : Google Storage, Compute Engine 
    Development : Client to storage the files and provide the access on-demand in GCP

    Generate the Google Storage and Generate the compute engine for performance
    Written by Kyndryl for gcp Data store location in Nature Labs Project
    Author : ramamurthy.valavandan@kyndryl.com
    gcloud components 
"""
ipfile="vaccination-data.csv"
#ifl="Latest reported counts of cases and deaths"

currentdirds="ds3"
coviddir="covid19"
basepath = "C:\\nature-labs\\who"
gcli="WHO"

#C:\nature-labs\who\covid19\ds3
projectID="tracing-matrix"
dataset="covid19"

URI="https://drive.google.com/file/d/132PDmI2o9gParYa4F23o_IqdR8Ncxo0J/view?usp=sharing"

ifc="Vaccination data"

sl=27
ls=len(ifc)
if(ls <= sl ):
    ifl=ifc
else:
    ifl=(ifc[0:sl])

"""
NO CHANGE SHOULD BE DONE AFTERWARDS ...
"""

gcloudcodepaths = ("{}{}{}{}{}".format(basepath,"\\",coviddir,"\\",currentdirds))

chkwho = ("{}{}{}".format(gcloudcodepaths,"\\",ipfile))

import re
import glob
from tkinter import W
import pandas as pd

from pandas import ExcelWriter
from pandas import ExcelFile
from os.path import expanduser as ospath

from pathlib import Path
import logging
import socket
from inspect import getsourcefile

import chardet
import pandas as pd

from datetime import datetime

import shutil
import xlrd

import runpy

import os
import sys

logf ="gcplog.txt"
logfi = ("{}{}".format("\\",logf))
logfile = (gcloudcodepaths + logfi)

logging.basicConfig( 
        filename = logfile, 
        level = logging.INFO, 
        format = '%(levelname)s:%(asctime)s:%(message)s')

logging.info('Compute Engine Directory: %r', {gcloudcodepaths})

fileinfo=(os.path.split(sys.argv[0])[1])
hostname=(socket.gethostbyaddr(socket.gethostname())[0])
datestamp = datetime.now().date()

logging.info('------Start of Google Log Analytics Projects ---------')
logging.info('Host Name %r, Compute Engine = %r', hostname, fileinfo)

path = Path(chkwho)

def prt(p):

    width = len(p) + 4
    print('┏' + "━"*width + "┓")
    print('┃' + p.center(width) + '┃')
    print('┗' + "━"*width + "┛")

if path.is_file():
    pi="\'Excel file is created  \' :"
    p = ("{} {}".format(pi,chkwho))
    prt(p)
    
else:
    pi="\'excelfilefordtye is missing !\' :"
    p = ("{} {}".format(pi,chkwho))
    prt(p)
    logging.error('Could not find xls file : %r', {p})
    exit(1)

targetdir = ("{}{}{}".format(basepath,"\\",currentdirds))


sno=("{}_{}".format(gcli,ifc))
#sno=("{}_{}".format(gcli,ifc))
dc=sno
dc = re.sub('[^A-Za-z0-9]+', ' ', dc)
dc = dc.strip()
dc = dc.rstrip()
dc = dc.lstrip()
dc = re.sub("\s", "_", dc)  

N="\\"
csvout = ("{}{}.{}".format(N,dc,"csv"))

pi = "\'Before Renaming \' :"
p = ("{}\t{}".format(pi,chkwho))

prt(p)

csvfileforuploadcsv = (gcloudcodepaths + csvout)

pi="\'After Renaming \' : "
p = ("{}\t{}".format(pi,csvfileforuploadcsv))
prt(p)
#print(chkwho)
#print(csvfileforuploadcsv)
shutil.copy(chkwho, csvfileforuploadcsv)

sno=("{}_{}".format(gcli,ifl))
fno=("{}_{}".format(gcli,ifc))
#sno=("{}_{}".format(gcli,ifc))
filetylst=['sql','csv','xlsx']
dc=fno
dc = re.sub('[^A-Za-z0-9]+', ' ', dc)
dc = dc.strip()
dc = dc.rstrip()
dc = dc.lstrip()
dc = re.sub("\s", "_", dc) 
ext_table_name=dc 
N="\\"
for fl in (filetylst):
    thr=("{}.{}".format(dc,fl))
    if(fl == 'sql'):
        bqf = ("{}{}{}".format(N,'BQ_',thr))
    if(fl== 'csv'):
        incsv = ("{}{}".format(N,thr))
        csvout = ("{}{}{}{}".format(N,'Upload_','GCP_',thr))
    else:
        outxls = ("{}{}".format(N,thr))

infc = (gcloudcodepaths + incsv)
conxls = (gcloudcodepaths + outxls)
csvfileforupload = (gcloudcodepaths + csvout)
bqfile = (gcloudcodepaths + bqf)
excelfilefordtye=csvfileforupload

with open(infc, 'rb') as f:
    enc = chardet.detect(f.read())  
    
dfc = pd.read_csv(infc, encoding = enc['encoding'])

dfc.to_excel(conxls, sheet_name=sno, index=False)

#dfc.close()


#print("Geneated Google Log Analytics Projects : ", conxls)

excelfilefordtye = conxls

xl = pd.ExcelFile(excelfilefordtye)

sheetlst=xl.sheet_names  

for sn in (sheetlst):
    sheetname=sn
    logging.info('Sheet Name : %r', sheetname)

#df = pd.read_excel(ospath(excelfilefordtye), sheet_name=sheetname)

with open(excelfilefordtye, "rb") as f:
     df_input_file = pd.read_excel(f, sheet_name=sheetname, header=0, index_col=None)


colname=df_input_file.columns
datatypes=dict(df_input_file.dtypes)
#print ("dty", datatypes)
row_count=df_input_file.count()[0]


logging.info('No of rows in Input File %r, Row Count %r', excelfilefordtye, row_count)


logging.info('\nGenerating the Project account in GCP:\n')


logging.info('Google Log Analytics Projects Generated File %r', conxls)

df_input_file.head(row_count).to_csv(csvfileforupload, encoding='utf-8', header=False, index=False)



def switch(check_data_type):
    dict={
              'object': 'STRING',
              'int64' : 'INT64',
              'float64': 'FLOAT64',
              'DATE'  : 'DATE'
          }
    return dict.get(check_data_type, 'Unable to find Data Type')


datearray=['date', 'DATE', 'Date']
fldnames=[]
for fld in colname:
    #dtdef=df[fld].dtypes
    #check_data_type = str(dtdef)
    for cdatesrt in (datearray):
        check_date_return = fld.find(cdatesrt)
        check_date_lu=cdatesrt
        if(check_date_return != -1):
            check_data_type='DATE'
            break
            #print ("Field_Name Data_Type, withoutstr", fld, check_data_type, check_date_lu, check_date_return) 
        else:
            dtdef=df_input_file[fld].dtypes
            check_data_type = str(dtdef)
        logging.info('Field Name %r, Check Data Type %r, Check DATE Return code %r', {fld}, {check_data_type}, {check_date_return})
        #print("Field, Check_Date_Str, check_date_ret",fld, check_data_type, check_date_return)
  

    flddty=switch(check_data_type)
    pi="\'Check Data Type of Field is Date:\' : "
    p = ("{}:{}:{}".format(pi,fld,flddty))
    #prt(p)
    dc=fld
    #cleaning(dc)
    dc = re.sub('[^A-Za-z0-9]+', ' ', dc)
    dc = dc.strip()
    dc = dc.rstrip()
    dc = dc.lstrip()
    dc = re.sub("\s", "_", dc)  
    dc = re.sub(r"[^\w\s]", '', dc)
    dc = re.sub(r"\s+", '_', dc)
    ddc=dc
    logging.info('Field Name : %r , Data Type :%r ', dc, flddty)
    logging.info('Fld Name : %r, Original DTy %r: Converted DTy is : %r', ddc, check_data_type, flddty)
    tblsting=("{} {}".format(dc, flddty))
    fldnames.append(tblsting)
      
logging.info('Elements in Table Field and Datatype %r', fldnames)
L=[]
lc=1
ll=len(fldnames)
logging.info('Number of Elements in Tbl Fld and Dty List or Array %r', ll)

for fldy in (fldnames):
    logging.info('Field and DTy :%r ', fldy)
    fldy=("{}\t{}".format("\t",fldy))
    L.append(fldy)
    if (lc == ll):
        N="\n"
    else:
        N=",\n"
    lc += 1
    L.append(N)

#print(bqfile)
cene = open(bqfile, 'w')

#tbe=sheetname
tbe=ext_table_name
tbe= re.sub('[^A-Za-z0-9]+', ' ', tbe)
tbe = tbe.strip()
tbe = tbe.rstrip()
tbe = tbe.lstrip()
tbe = re.sub("\s", "_", tbe)  
tbe = re.sub(r"[^\w\s]", '', tbe)
tbe = re.sub(r"\s+", '_', tbe)
   
ts=("{} {} {}".format("--Generated schema for table:",tbe,"--"))
fulltblname=("{}.{}.{}".format(projectID,dataset,tbe))
line1=("{} {}{}{}".format("CREATE EXTERNAL TABLE","`",fulltblname,"`\n"))

#print(line1)

line2="(\n"
line3="\n)"
s = """
OPTIONS(
skip_leading_rows=0,
format="CSV",
"""
uris=("{}{}{}{}".format("uris=[","\"",URI,"\"]"))
line4="\n);"
#cene.write(ts)

cene.write(line1)
cene.write(line2)
cene.writelines(L)

cene.write(line3)

cene.write(s)

cene.write(uris)

cene.write(line4)

cene.close()

logformatfile = ("{}{}_{}_{}".format("\\",hostname,datestamp,logf))
logdfilenew = (gcloudcodepaths + logformatfile)
#print("After Renaming:", logdfilenew)
shutil.copy(logfile, logdfilenew)

postscript="cleanfiles.py"
cfls = ("{}{}".format("\\",postscript))
cleanfile = (gcloudcodepaths + cfls)
clean = open(cleanfile, 'w')

def cleanfl (rmv, removefile):

    #print(removefile)

    ldc = str(removefile)
    slfs = (ldc.split('\\'))

    leba=len(slfs)-1
    for rf in range(0,len(slfs)):
        
        if(rf == 0):
            sla=("{}{}".format(rmv," = \""))
    
        else:
            sla=("{}{}".format("\\","\\"))
        arf=("{}{}".format(sla,slfs[rf]))
        larys.append(arf)
        if(rf == leba):
            dq=("{}{}".format("\"","\n"))
            larys.append(dq)
    clean.writelines(larys)

removefile=logfile
larys=[]
cleanfl('logfile_rm',removefile)

#print(excelfilefordtye)

larys=[]
cleanfl('excel_rm',excelfilefordtye)

s = """
import shutil
import os
import sys

#remove file if exists
def remove_if_exists(removefile):
    try:
        if os.path.exists(removefile):
            os.remove(removefile)
            print ("File removed successfully", removefile)
    except:
        print("Error while deleting file ", removefile)

#remove previous log file

removefile = logfile_rm
remove_if_exists(removefile)

removefile = excel_rm
remove_if_exists(removefile)

"""
clean.write(s)

clean.close

#runpy.run_path(path_name='script-01.py')

pi="\'Create 'Table' GCP Big Query \' : "
p = ("{} {}".format(pi,bqfile))
prt(p)

pi="Execute 'python' for cleaning file(s) : "
p = ("{} {}".format(pi,cleanfile))
prt(p)

pi="python "
p = ("{} {}".format(pi,cleanfile))
prt(p)
#runpy.run_path(path_name = cleanfile)