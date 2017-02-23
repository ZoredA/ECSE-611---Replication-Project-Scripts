import sys
import sqlite3
import subprocess
from subprocess import STDOUT
from subprocess import CalledProcessError, TimeoutExpired
from xmlrpc import client
import re
import os
import shutil
rpc_client = client.ServerProxy('https://pypi.python.org/pypi')


#First query the database

#Checks if pip can install the python3 version of a given package/version
def getIfPython3(package, version):
    try:
        data = rpc_client.release_data(package, version)
    except Exception as e:
        print(Exception)
        raise IndexError("Likely socket fault")
    classifiers = data.get('classifiers', None)
    if not classifiers:
        raise IndexError("No classifiers found.")
    python3_list = [x for x in classifiers if 'Python :: 3' in x]
    if len(python3_list) > 0:
        return True
    python2_list = [x for x in classifiers if 'Python :: 2' in x]
    if len(python2_list) == 0:
        raise IndexError("No Python 2 classifiers found.")
    return False

class DependencyDL:
    def __init__(self):
        self.conn = sqlite3.connect('pypi_filtered.db')
        self.c = self.conn.cursor()
        #self.c.execute('SELECT COUNT(*) from PACKAGE_INDEX')
        #self.max_count = self.c.fetchone()[0]
        #print("DB SIZE: %s" % self.max_count)
        self.omit_list = []
        self.other_con = sqlite3.connect('pypi_filtered_again.db')
        self.cur_small = self.other_con.cursor()

    #This returns a single package tuple. 
    def get_package(self):
        select_query = r"""
            SELECT package,version,date,downloads FROM PACKAGE_INDEX
             
        """
        self.c.execute(select_query)
        tup = self.c.fetchone()
        while (tup):
            yield tup
            tup = self.c.fetchone()
        
    #This function works on a list of selected packages,
    #rather than the whole table. 
    def get_selected_packages(self, package_list):
        select_package_query = r"""
            SELECT package,version,date,downloads FROM PACKAGE_INDEX where package=?
            order by date desc
        """
        for package in package_list:
            print(package)
            self.c.execute(select_package_query, (package,))
            tup = self.c.fetchone()
            while tup:
                yield tup
                tup = self.c.fetchone()
                
    
    def fil_db_check(self, package_tup):
        q = "select * from package_index where package=? and version=?"
        self.cur_small.execute(q, (package_tup[0], package_tup[1]))
        if self.cur_small.fetchone():
            return True
        return False
    
    TWO_LIST=[]
    TEMP_DIR = r"/mnt/Meow/Temp"
    OUTPUT_DIR = r"/mnt/Meow/DepStorage/"
    FILE_SET = set(os.listdir(OUTPUT_DIR)).union(os.listdir(r"/home/zored/tmp/Fri/DepStorage")) 
    #FILE_SET = FILE_SET.union(os.listdir(r"/mnt/Meow/DepStorage2"))
    FILE_SET = FILE_SET.union(os.listdir(r"/home/zored/tmp/Fri/DepStorage2"))

    #This runs the external pip command which outputs to STDOUT
    #which we then somehow parse and return as a list of strings.
    #These strings will then have to be regexed to get the package
    #and its version.
    #@rate_limited(1)
    def run_command(self, package_tup):
        pip_arg = package_tup[0] + "==" + package_tup[1]
        if package_tup[0] + "_" + package_tup[1] in self.FILE_SET:
            print(pip_arg+ " appears to exist. Not redownloading.")
            return
        if 'ERROR_'+ package_tup[0] + "_" + package_tup[1] in self.FILE_SET:
            print(pip_arg + "appears to have errored. Not redownloading.")
            return
        if self.fil_db_check(package_tup):
            print(pip_arg + "in db already")
            return

        output_path = os.path.join(self.OUTPUT_DIR, package_tup[0] + "_" + package_tup[1])
        error_path = os.path.join(self.OUTPUT_DIR, 'ERROR_' + package_tup[0] + package_tup[1])
        #if os.path.exists(output_path):
            #print(pip_arg+ " appears to exist. Not redownloading.")
            #return
        #if os.path.exists(error_path):
            #print(pip_arg+ " appears to have errored. Not redownloading.")
            #return
        try:
            python_three = getIfPython3(package_tup[0], package_tup[1]) 
        except IndexError as e:
            self.omit_list.append( package_tup[0] + "==" + package_tup[1] )
            return

        error = False
        output = None
        try:
            print(pip_arg)
            if python_three is False:
                #pip3 download $PACKAGE -d /mnt/Meow/Temp/ --no-binary :all: \
                print(pip_arg + " is python 2 only")
                #output = subprocess.check_ouput("pip3 download " + pip_arg + " -d /mnt/Meow/Temp/ --no-binary: all:")
                #output = subprocess.check_output("./getDep.sh %s" % pip_arg, shell=True)
                #output = subprocess.check_output("pip download " + pip_arg + " -d " + self.TEMP_DIR +  " --no-binary :all:", shell=True, stderr=STDOUT)
                self.TWO_LIST.append(pip_arg)
                return
            else:
                output = subprocess.check_output("pip3 download " + pip_arg + " -d " + self.TEMP_DIR + " --no-binary :all:", shell=True, stderr=STDOUT, timeout=40)
                #output = subprocess.check_output("./getDep3.sh "+ pip_arg, shell=True, stderr=STDOUT)
        #http://stackoverflow.com/a/16198668
        except (CalledProcessError, TimeoutExpired) as exc:   
            #print(output)
            print("Failed %s" % pip_arg, exc.output)
            error = True
            with open(error_path, 'w') as f:
                f.write(exc.output.decode("utf-8"))
        

        if os.path.exists(self.TEMP_DIR):
            shutil.rmtree(self.TEMP_DIR)

        if error is False:
            output = output.decode("utf-8") 
            print(output[0:100] +"...done")
            with open(output_path, 'w') as f:
                f.write(output)
                
    def get_most_popular(self):
        q = "SELECT * from PACKAGE_INDEX GROUP BY package order by downloads desc LIMIT 100 OFFSET 29"
        self.c.execute(q)
        l = self.c.fetchall()
        return l
       
    def get_newest_versions(self):
        q = """select t1.package,t1.version, t1.newest_date as newest_date, dl_sum as downloads from 

        (select package,version, max(date)  as newest_date from PACKAGE_INDEX
        group by package
        order by package desc) t1
        INNER JOIN 

        (select sum(downloads) as dl_sum , package from PACKAGE_INDEX
            group by package having dl_sum > 5000
                order by dl_sum desc ) t2
                ON t1.package=t2.package
        Order by downloads asc
        """
        self.c.execute(q)
        tup = self.c.fetchone()
        while tup:
            yield tup
            tup = self.c.fetchone()

import json

def run():
    #aList = ['simplejson']
    d = DependencyDL()
    for tup in d.get_package():
        d.run_command(tup)
    
    with open('omit_noPy2.js','w') as f:
        json.dump(d.omit_list,f)

    PY_TWO = os.path.join(d.OUTPUT_DIR, "py2.js")
    with open(PY_TWO, 'w') as f:
        json.dump(d.TWO_LIST,f)

def run_new_popular():
    d = DependencyDL()
    for tup in d.get_newest_versions():
        d.run_command(tup)

    with open('omit_noClassifier_2Pop.js','w') as f:
        json.dump(d.omit_list,f)

    PY_TWO = os.path.join(d.OUTPUT_DIR, "py2OnlyPop_2.js")
    with open(PY_TWO, 'w') as f:
        json.dump(d.TWO_LIST,f)

def run_popular():
    d = DependencyDL()
    aList = [x[0] for x in d.get_most_popular()]
    for tup in d.get_selected_packages(aList):
        d.run_command(tup)
        
    with open('omit_noClassifier_2.js','w') as f:
        json.dump(d.omit_list,f)

    PY_TWO = os.path.join(d.OUTPUT_DIR, "py2Only_2.js")
    with open(PY_TWO, 'w') as f:
        json.dump(d.TWO_LIST,f)
