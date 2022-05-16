import requests
import re
import shutil
import os
import sys

who_data_url = 'https://covid19.who.int/who-data/vaccination-data.csv'
ds3_code_url = 'https://drive.google.com/file/d/1BTPLn3dqn5fTinctuVJ_i3H-YPqSJ53j/view?usp=sharing'

whodata=re.sub(r'^.+/([^/]+)$', r'\1', who_data_url)

code_name="whods3proc.py"

workingdirctory="ds3"
customerdirctory="covid19"
basepath = "C:\\nature-labs\\who"
gcloudcodepaths = ("{}{}{}{}{}".format(basepath,"\\",customerdirctory,"\\",workingdirctory))

fullyqualifiedwhodata = ("{}{}{}".format(gcloudcodepaths,"\\",whodata))

def prt(p):

    width = len(p) + 4
    print('┏' + "━"*width + "┓")
    print('┃' + p.center(width) + '┃')
    print('┗' + "━"*width + "┛")

#remove file if exists
def remove_if_exists(removefile):
    try:
        if os.path.exists(removefile):
            os.remove(removefile)
            #print ("File removed successfully", removefile)
            pi="\'File removed successfully \' :"
            p = ("{}{}".format(pi,removefile))
            prt(p)
    except:
        print("Error while deleting file ", removefile)

#remove previous log file

removefile = fullyqualifiedwhodata
remove_if_exists(removefile)

pi="\'Downloading WHO Vaccination data \' :"
p = ("{}{}".format(pi,who_data_url))
prt(p)

def downloading(download_url,local_file_data):
    file_stream = requests.get(download_url, stream=True)
    with open(local_file_data, 'wb') as local_file:
        for data in file_stream:
            local_file.write(data)

download_url=who_data_url
local_file_data=fullyqualifiedwhodata
downloading(download_url,local_file_data)

pi="\'Download is completed : \' :"
p = ("{}{}".format(pi,fullyqualifiedwhodata))
prt(p)

download_url=ds3_code_url
local_file_data=code_name
downloading(download_url,local_file_data)
