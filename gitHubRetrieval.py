from github3 import login, GitHub
from getpass import getpass, getuser
import sqlite3
import sys
import os
import requests 
import getDep
from requirements_detector import find_requirements
import re
regex = re.compile('[><=!]')

con = sqlite3.connect('pypi_filtered.db')
cur = con.cursor()
cur.execute('select * from PACKAGE_INDEX group by PACKAGE')
#packages = cur.fetchall()
full_dict = {x[0].lower():x for x in cur.fetchall()}

con.close()

con = sqlite3.connect('pypi_filtered_again.db')
cur = con.cursor()
cur.execute('select * from PACKAGE_INDEX group by PACKAGE')
gotten = set(x[0].lower() for x in cur.fetchall())

need = full_dict.keys() - gotten
print(next(iter(need)))

def githubWork():
    try:
        import readline
    except ImportError:
        pass

    try:
        user = raw_input('GitHub username: ')
    except KeyboardInterrupt:
        user = getuser()

    password = getpass('GitHub password for {0}: '.format(user))

    # Obviously you could also prompt for an OAuth token
    if not (user and password):
        print("Cowardly refusing to login without a username and password.")
        sys.exit(1)

    g = login(user, password)

    def do_work():
        for package in need:
            results = g.search_repositories('%s in:name stars:>=500 fork:true language:python' % (package) )

            
def try_setup_analysis():
    err_file = 'failed_setup.txt'
    if os.path.exists(err_file):
        with open(err_file) as f:
            failed = set(f.read().split('\n'))
    else:
        failed = set()
    template = r"https://raw.githubusercontent.com/%s/%s/master/setup.py"
    with open(err_file, 'a') as f:
        for index, package in enumerate(need):
            package_tup = full_dict[package]
            
            if package in failed:
                continue
            setup_path = os.path.join('setups', 'setup.py')
            url = template % (package, package)
            output_list = []
            if index % 100 == 0:
                f.flush()
                print('committing')
                con.commit()
            try:
                req = requests.get(url, timeout=2)
                if req.status_code != 200:
                    f.write(package)
                    f.write('\n')
                    continue
                with open(setup_path, 'wb') as output:
                    print('found setup.py for %s' % package)
                    output.write(req.content)
                    try_ours = False
                    #Run the analysis tool here
                    try:
                        tool_output = find_requirements('setups')
                    except Exception:
                        print('Could not use req parser. Trying ours')
                        try_ours = True
                        tool_output = False
                    if tool_output:
                        mini_set = set()
                        for requ_obj in tool_output:
                            requirement = requ_obj.requirement.project_name
                            specifiers = ','.join(''.join(x) for x in requ_obj.requirement.specs)
                            if (requirement, specifiers) in mini_set:
                                continue
                            mini_set.add (  (requirement, specifiers) )
                            tup = (package, package_tup[1], requirement, specifiers)
                            print(tup)
                            output_list.append(tup)
                        write_dependency(package_tup, output_list)
                    elif try_ours is True:
                        try:
                            deps = getDep.parse_setup(setup_path)
                        except Exception as e:
                            print(e)
                            print('failed %s' % package)
                            continue
                        if deps:
                            dep_list = get_dependencies(package, version, deps)
                            dep_set(dep_list) #Makes sure each tuple is unique!
                            write_dependency(package_tup, dep_set)
                        elif deps is None:
                            print('failed %s' % package)
                        else:
                            print("we think %s is a dependency less package" % package)
                            write_dependency(package_tup, None)
                    
                print('Got something for %s' % package)
            except requests.exceptions.Timeout:
                print('%s timed out' % package)
            except requests.exceptions.ConnectionError:
                print('%s got a connenction error. Exiting' % package)
                raise
    con.commit()
    con.close()
                
def get_dependencies(package, version, dep_list):
    ret_deps = []    
    for dep in dep_list in f:
        match = regex.search(line)
        if match:
            (dependency,dep_version) = ( line[0:match.start()].strip(), line[match.start():] .strip())
            if dependency.lower() not in full_set:
                print(dependency + ' not found in full set. Skipping ')
                continue
            ret_deps.append( (package, version, dependency, dep_version) )
        else:
            dep = dep.strip()
            if dep.lower() not in full_set:
                print(dep + ' version less not found in full set. Skipping ')
                continue
            ret_deps.append( (package, version, line,'') ) 
    return ret_deps
                
def write_dependency(package_tup, tup_list):
    if tup_list:
        q = "Insert into Dependencies(package, version, dependency, dependency_version) values (?,?,?,?)"
        cur.executemany(q, tup_list)
    q = "Insert into PACKAGE_INDEX values (?,?,?,?)"
    cur.execute(q, package_tup)
                
def try_raw_requirements():
    if os.path.exists('failed.txt'):
        with open('failed.txt','r') as f:
            failed = set(f.read().split('\n'))
    template = r"https://raw.githubusercontent.com/%s/%s/master/requirements.txt"
    with open('failed.txt','w') as f:
        for index, package in enumerate(need):
            package = package[0]
            if package in failed:
                continue
            pa = os.path.join('requirements',package+'_requirements.txt')
            url = template % (package, package)
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
                print('Got something for %s' % package)
            except requests.exceptions.Timeout:
                print('%s timed out' % package)
            except requests.exceptions.ConnectionError:
                print('%s got a connenction error. Exiting' % package)
                raise
            