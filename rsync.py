import os
import os.path
import utilities
import subprocess
from subprocess import Popen, PIPE
import re
from datetime import datetime

path = os.path.abspath('./rsync.yaml')
configs = utilities.read_yaml(path)
bucket_name = configs['Bucket_Name']
number_of_process = configs['Number_of_Process']
number_of_thread = configs['Number_of_Thread']
number_of_keys = configs['Number_of_Keys']
Email_ID = configs['Email_ID']
source_host=configs['Source_Host_Details']['Host_Name']
destination_host=configs['Destination_Host_Details']['Host_Name']
DB_IP = configs['DB_IP']
source_url=configs['Source_Host_Details']['endpoint_url']
destination_url=configs['Destination_Host_Details']['endpoint_url']
host_connect_timeout=configs['Host_Connect_Timeout']
read_connect_timeout=configs['Read_Connect_Timeout']

list_dir=[]
spath = "/root/src"
dpath = "/root/dst/"

def rsync_copy(srcpath,dpath):
    logfile = re.sub(r'[\W/]','_', srcpath)
    final_logile = datetime.now().strftime('rsync'+logfile+'_%H_%M_%d_%m_%Y.log')
    #print final_logile
    process = Popen(['/usr/bin/rsync','-arP',srcpath,dpath], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    print stdout

if __name__ == "__main__":
    for d in os.listdir(spath):
       if os.path.isdir(os.path.join(spath, d)):
          path1=spath+"/"+d
          list_dir.append(path1)
    #print list_dir

    for i in list_dir:
        rsync_copy(i,dpath)

"""
for i in list_dir:
    process = Popen(['/usr/bin/rsync','-arP',i,dpath], stdout=PIPE, stderr=PIPE)
    #stdout, stderr = process.communicate()
    #print stdout
    output = process.communicate()[0]
    print output

#for i in list_dir:
#     file = os.system('/usr/bin/rsync  -ar ' +i+" "+ dpath)
#     print file


"""

"""
for f in dirlist:
    if os.path.isdir(f):
        print os.path.abspath(f)
        list_dir.append(os.path.abspath(f))
print list_dir
list_len=len(list_dir)

if list_len < number_of_process:
     print "run rsync "
     #os.system("rsync -avrz /opt/data/filename /")
"""
#dirs = [d for d in os.listdir(spath) if os.path.isdir(os.path.join(spath, d))]
#print dirs
