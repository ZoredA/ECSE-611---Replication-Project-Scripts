import os
import requests
import sqlite3 
import re
from dateutil.parser import *

#This retrieves past requirements.txts!
req_folder = r'requirements'
reqs = os.listdir(req_folder)
regex = re.compile('[><=!]')

con = sqlite3.connect('pypi_filtered.db')
cur = con.cursor()
cur.execute('select package from PACKAGE_INDEX group by package')
full_set = set([x[0].lower() for x in cur.fetchall()])
max_len = len ( max(full_set, key=len) )

print("Size of full set: %s" % len(full_set))
print("Sample: %s" % next(iter(full_set)))
con.close()

con = sqlite3.connect('pypi_filtered_again.db')
cur = con.cursor()

def get_dependencies(file_path, package, version):
    deps = []
    with open(file_path) as f:
        for line in f:
            match = regex.search(line)
            if match:
                (dependency,dep_version) = ( line[0:match.start()].strip(), line[match.start():] .strip())
                if dependency.lower() not in full_set:
                    print(dependency + ' not found in full set. Skipping ')
                    continue
                deps.append( (package, version, dependency, dep_version) )
            else:
                line = line.strip()
                if line.lower() not in full_set:
                    print(line + ' version less not found in full set. Skipping ')
                    continue
                deps.append( (package, version, line,'') ) 
    return deps

def run_tabulate_no_ver():
    for req in reqs:
        package = os.path.splitext(req)[0].split('_')[0]
        try:
            data = requests.get('http://pypi.python.org/pypi/%s/json' % package)
        except Exception as e:
            attempts += 1
            if attempts > 3:
                raise
            else:
                continue
        attempts = 0
        js = data.json()
        info = js['info']
        version = info['version']
        info['date'] = js['releases'][version][-1]['upload_time']
        info['downloads'] = js['releases'][version][-1]['downloads']
        
        store_requirements(os.path.join(req_folder, req), info)
    con.commit()
        
def get_more():
    for req in reqs:
        package = os.path.splitext(req)[0].split('_')[0]
        try:
            data = requests.get('http://pypi.python.org/pypi/%s/json' % package)
        except Exception as e:
            print('failed to get pypi data. Retrying')
            attempts += 1
            if attempts > 3:
                raise
            else:
                continue
        attempts = 0
        js = data.json()
        info = js['info']
        version = info['version']
        try:
            info['date'] = js['releases'][version][-1]['upload_time']
            info['downloads'] = js['releases'][version][-1]['downloads']
        except IndexError:
            print("skipping %s due to index error" % package)
            print(info)
            continue
        versions = js['releases'].keys()
        need = [ (package, x) for x in js['releases'].keys() if x != version and not check_db(package,x) ]
        if need:
            files = try_raw_requirements_version(need)
            for f in files:
                f_info = {}
                f_package = f[0]
                f_version = f[1]
                file_path = f[2]
                try:
                    f_info['date'] = js['releases'][f_version][-1]['upload_time']
                    f_info['downloads'] = js['releases'][f_version][-1]['downloads']
                    f_info['name'] = f_package
                    f_info['version'] = f_version
                except IndexError:
                    print('Could not get good json data for version: %s==%s' % (package, version))
                    print('Skipping')
                    continue
                store_requirements(file_path, f_info)
            con.commit()
            print('done %s' % package)
        

 
#This stores the requirement data into the DB
def store_requirements(file_path, package_info):
    downloads = package_info['downloads']
    date = package_info['date']
    version = package_info['version']
    package = package_info['name']
    date = parse(date) #Makes sure we get it in an sqllite friendly form.
    q = r"INSERT INTO PACKAGE_INDEX (package, version, date, downloads) values (?,?,?,?)"
    if not check_db(package, version):
        deps = get_dependencies(file_path, package, version)
        cur.execute(q, (package,version,date,downloads))
        cur.executemany('INSERT INTO DEPENDENCIES(package,version, dependency, dependency_version) values (?,?,?,?)', deps)
    else:
        print('%s,% already in db. Skipping' % (package, version))
        
        
def check_db(package, version):
    q = "select * from PACKAGE_INDEX where package = ? and version = ?"
    cur.execute(q, (package, version))
    if cur.fetchone():
        return True
    return False
    
def try_raw_requirements_version(need):
    if os.path.exists('failed.txt'):
        with open('failed.txt','r') as f:
            failed = set(f.read().split('\n'))
    ret_list = []
    template = r"https://raw.githubusercontent.com/%s/%s/%s/requirements.txt"
    with open('failed.txt','w') as f:
        for index, tup in enumerate(need):
            package = tup[0]
            version = tup[1]
            if package in failed:
                continue
            pa = os.path.join('requirements_ver',package+'_'+version+'_requirements.txt')
            url = template % (package, package, version)
            if os.path.exists(pa):
                continue
            if index % 100 == 0:
                f.flush()
            try:
                req = requests.get(url, timeout=2)
                if req.status_code != 200:
                    f.write(package)
                    f.write('\n')
                    continue
                with open(pa, 'wb') as output:
                    output.write(req.content)
                ret_list.append((package, version, pa))
                print('Got something for %s' % package)
            except requests.exceptions.Timeout:
                print('%s timed out' % package)
            except requests.exceptions.ConnectionError:
                print('%s got a connenction error. Exiting' % package)
                raise
    return ret_list