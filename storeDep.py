#This file stores the downloaded files into the database
#after parsing the thingys out of them!
import os
import sqlite3
import re

file_path = r"/mnt/Meow/DepStorage2/"

class StoreDep:
    def __init__(self):
        self.file_path = file_path
        self.file_list = os.listdir(file_path) 
        self.conn = sqlite3.connect('pypi_filtered_again.db')
        self.c = self.conn.cursor()
        self.regex = re.compile('[><=!]')
        
        self.index_conn = sqlite3.connect('pypi_filtered.db')
        self.index_cur = self.index_conn.cursor()

    def get_index_entry(self, package, version):
        q = "select * from PACKAGE_INDEX where package=? and version=?"
        self.index_cur.execute(q, (package, version))
        return self.index_cur.fetchone()
        
    def insert_index_entry(self, tup):
        q = "insert into package_index values (?,?,?,?)"
        self.c.execute(q, tup)
        self.conn.commit()
        
    def check_new_index(self, package, version):
        q = "select * from PACKAGE_INDEX where package=? and version=?"
        self.c.execute(q, (package, version))
        if self.c.fetchone():
            return True
        return False
        
    def do_index_sync(self):
        print("Starting index sync")
        for f in self.file_list:
            if 'ERROR' in f:
                continue
            if f in ignore:
                continue
            if f[-3:] == '.js': continue
            try:
                (package,version) = f.rsplit('_', 1)
            except:
                print(f)
                raise
            #print(package, version)
            if self.check_new_index(package, version):
                continue
            tup = self.get_index_entry(package, version)
            if not tup or len(tup) < 2:
                print(tup)
                print(f)
                if  f.count('_') == 2:
                    print('spliting in other direction')
                    (package,version) = f.split('_', 1)
                    if self.check_new_index(package, version):
                        continue
                    tup = self.get_index_entry(package, version)
                else:
                    print('skipping')
                    continue
            self.insert_index_entry(tup)
            
        
    def do_work(self):
        for f in self.file_list:
            if 'ERROR' in f:
                continue
            if f in ignore:
                continue
            if f[-3:] == '.js': continue
            try:
                (package,version) = f.rsplit('_', 1)
            except:
                print(f)
                raise
            #print(package, version)

            if self.check_if_in_dep(package, version):
                print("%s %s in DB already. Skipping." % (package, version))
                continue
            i = self.get_dependencies(os.path.join(self.file_path, f), package, version)
            if i:
                print(i)
                self.write_to_db(i)

    def write_to_db(self, tup_list):
        query = "INSERT INTO DEPENDENCIES(package, version, dependency, dependency_version) values(?,?,?,?)"
        self.c.executemany(query, tup_list)
        self.conn.commit()

    def check_if_in_dep(self, package, version):
        query = "select * from DEPENDENCIES where package=? and version=?"
        self.c.execute(query, (package,version))
        if self.c.fetchone():
            return True
        return False
        
    def get_dependencies(self, file_path, package, version):
        package_str = package + "==" + version #this assumes we were passing package==version to pip.
        deps = []
        with open(file_path) as f:
            lines = [x for x in f if 'Collecting' in x and package_str in x and '->' not in x ]
            for line in lines:
                req = line.split(' ')[1]
                if package_str in req: 
                    continue #Skip lines like Collection package==version where package is the one being got.
                match = self.regex.search(req)
                if match:
                    (dependency,dep_version) = ( req[0:match.start()], req[match.start():] )
                    deps.append( (package, version, dependency, dep_version) )
                else:
                    deps.append( (package, version, req,'') ) 
        return deps

ignore = ['py2.js_new', 'py2.js', 'py2Only.js', 'py2Only_2.js', 'py2OnlyPop_2.js']
def run_store():
    s = StoreDep()
    s.do_work()
    
    # s.file_list = os.listdir(file_path2)     
    # s.file_path = file_path2
    # s.do_work()
    
    s.conn.close()
    
    s = StoreDep()
    s.do_index_sync()
    s.conn.close()
