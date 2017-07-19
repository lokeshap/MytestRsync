import os
import os.path
import utilities
import subprocess
from subprocess import Popen, PIPE
import re
from datetime import datetime
import multiprocessing

path = os.path.abspath('./rsync.yaml')
configs = utilities.read_yaml(path)
iterv = configs['Number_of_Folders']
fiterv = configs['Number_of_Files']
Email_ID = configs['Email_ID']
DB_IP = configs['DB_IP']
spath=configs['Source_Path']
dpath=configs['Destination_Path']


def rsync_copy(srcpath,dpath):
    print "Rsync started for folder : ", srcpath
    logfile = re.sub(r'[\W/]','_',srcpath)  #print "logfile" , logfile
    final_logile = datetime.now().strftime('rsync'+logfile+'_%H_%M_%d_%m_%Y.log')
    flog="/root/log/"+final_logile     #print flog
    fh = open(flog,"w")
    fh.write("#########################################\n")
    process = Popen(['/usr/bin/rsync','-arP','--progress',srcpath,dpath], stdout=PIPE) #, stderr=PIPE)
    #print "=====================================================\n"
    while process.poll() is None:
        #output = process.stdout.readline()
        output = process.communicate()[0]
        fh.write(output)
        #print output,

    #stdout, stderr = process.communicate()
    #print stdout
    fh.close()

def rsync_file(srcpath,dpath):
    print "Rsync started for file : ", srcpath
    logfile = re.sub(r'[\W/]','_',srcpath)  #print "logfile" , logfile
    final_logile = datetime.now().strftime('rsync'+logfile+'_%H_%M_%d_%m_%Y.log')
    flog="/root/log/"+final_logile     #print flog
    fh = open(flog,"w")
    fh.write("#########################################\n")
    print "====================================================="
    process = Popen(['/usr/bin/rsync','-arP','--progress',srcpath,dpath], stdout=PIPE) #, stderr=PIPE)
    while process.poll() is None:
        #output = process.stdout.readline()
        output = process.communicate()[0]
        fh.write(output)
        print output,
        print "====================================================="
    #stdout, stderr = process.communicate()
    #print stdout
    fh.close()

#def rsync_file():


"""
while proc.poll() is None:
    output = proc.stdout.readline()
    print output,
If ls ends too fast, then the while loop may end before you've read all the data.
You can catch the remainder in stdout this way:
output = proc.communicate()[0]
print output,
"""

if __name__ == "__main__":
    list_dir = []
    list_file = [] # os._exit(0)
    for d in os.listdir(spath):
       if os.path.isdir(os.path.join(spath, d)):
          path1=spath+"/"+d
          list_dir.append(path1)
       else:
           path2=spath+"/"+d
           list_file.append(path2)
    #print list_dir
    #print list_file
    #os._exit(0)
    #Findout the number of directories you want to send based on iterations
    if len(list_dir) % iterv == 0:
        iterations = len(list_dir) / iterv
    else:
        iterations = len(list_dir) / iterv + 1
    #Findout the number of files you want to send based on iterations
    if len(list_file) % fiterv == 0:
        fiterations = len(list_file) / fiterv
    else:
        fiterations = len(list_file) / fiterv + 1
    #Function copies folders
    cc = 1; i = 0; j = iterv
    #jobs = []
    while (cc <= iterations):
        b = list_dir[i:j]     #print b
        jobs = []
        for xxx in b: #print "Current folder == " , xxx  #print dpath
           p = multiprocessing.Process(target=rsync_copy, args=(xxx, dpath))
           jobs.append(p)
           print "jobs " , jobs
           p.start()
        #jobs = []
            #rsync_copy(xxx, dpath)
        i = j
        j = j + iterv
        cc += 1
    #Function copies files
    #fcc = 1;i = 0;j = fiterv
    #while (fcc <= fiterations):
    #   b = list_file[i:j]  # print b
    #   jobs = []
        #for xxx in b:  # print "Current folder == " , xxx  #print dpath
        #  p = multiprocessing.Process(target=rsync_file, args=(xxx, dpath))
        #  jobs.append(p)
        #  p.start()
                # rsync_copy(xxx, dpath)
    #   i = j
    #   j = j + fiterv
    #   fcc += 1
    #for i in list_dir:
    #   rsync_copy(i,dpath)
"""
def worker(num):
    #thread worker function
    print 'Worker:', num
    return

if __name__ == '__main__':
    jobs = []
    for i in range(5):
        p = multiprocessing.Process(target=worker, args=(i,))
        jobs.append(p)
        p.start()
"""
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
