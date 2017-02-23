#Get the list of packages.
#Iterate through them.
#Record important information out.
#Get following:
#package name, version, index,
import sqlite3
import json
from datetime import datetime

index_insert = r"""
    INSERT INTO PACKAGE_INDEX (package, version, date, downloads)
    VALUES(?,?,?,?)
"""

dependency_insert = r"""
    INSERT INTO DEPENDENCIES (package, version, dependency, dependency_version)
    VALUES(?,?,?,?)
"""

class PackageDL:
    conn = None
    c = None
    def __init__(self):
        self.conn = sqlite3.connect('pypi.db')
        self.c = self.conn.cursor()

    def create_tables(self):
        c = self.c
        c.execute(self.Index_Table_Create)
        c.execute(self.Dep_Table_Create)

    #We create three tables. 
    #An index which contains a package name
    Index_Table_Create = r"""
        CREATE TABLE IF NOT EXISTS PACKAGE_INDEX (
            package TEXT NOT NULL, 
            version TEXT NOT NULL, 
            date TIMESTAMP NOT NULL, 
            downloads NUMER NOT NULL,
            PRIMARY KEY(package, version)
        )
    """

    Dep_Table_Create = r"""
        CREATE TABLE IF NOT EXISTS DEPENDENCIES (
            package TEXT NOT NULL, 
            version TEXT NOT NULL, 
            dependency TEXT NOT NULL,
            dependency_version TEXT NOT NULL,
            PRIMARY KEY(package, version)
        )
    """

    versions_omitted = []
    #This downloads a given package. Include the version number in
    #parameter if you wish to download an older version (it gets passed
    #straight to pip)
    def download_package(self, package):
        pass

    #This writes out the package information to the index table.
    #Expects a dictionary (such as the one returned by the JSON API)
    #containing the necessary information.
    def write_to_index(self, package_data):
        try:
            package = package_data['info']['name']
            newest_version = package_data['info']['version']
            timestamp = package_data['urls'][0]['upload_time'] #we pick an arbitrary one.
            timestamp = getDateTime(timestamp) #Converts it into a datetime object.

            urls_sum = sum(url['downloads'] for url in package_data['urls'])
            if urls_sum == 0:
                newest_release_dict = package_data['releases'][newest_version]
                releases_sum = sum(release['downloads'] for release in newest_release_dict)
                downloads = releases_sum #This is likely to also be 0.
            else:
                downloads = urls_sum
            arg_list = [(package, newest_version,timestamp, downloads)]
            releases = package_data['releases'] #Dictionary of releases
            for version in releases:
                if version == newest_version: continue
                if not releases[version]: 
                    print("No info found for %s version %s" % (package, version))
                    self.versions_omitted.append( package+version )
                    continue
                release_list = releases[version] #A list of dictionaries
                downloads_sum = sum(release['downloads'] for release in release_list)
                timestamp = release_list[0]['upload_time']
                timestamp = getDateTime(timestamp) #Converts it into a datetime object.
                arg_list.append( (package, version, timestamp, downloads_sum) ) 
            self.c.executemany(index_insert, arg_list)
            self.conn.commit()
        except Exception as e:
            print(package_data['info'])
            raise e

    #Writes out a package and its dependencies to a table.
    #Expects dependency_ver_list to be a list of tuples where the first one
    #is the package and the second is the version (can include the operators
    #because they are important).
    def write_to_dependency(self, package, version, dependency_ver_list):
        pass

    #We only check the index. We do atomic writes and make sure the index table
    #is written to last....
    def check_db(self, package, version):
        c = self.c
        query = r"""
            SELECT * FROM PACKAGE_INDEX where package=? and version=?
        """
        c.execute(query, (package,version))
        return c.fetchone() > 0

#Original code taken from:
#https://wiki.python.org/moin/PyPISimple
from xml.etree import ElementTree
from urllib.request import urlopen

def get_distributions(simple_index='https://pypi.python.org/simple/'):
    with urlopen(simple_index) as f:
        tree = ElementTree.parse(f)
    return [a.text for a in tree.iter('a')]

def scrape_links(dist, simple_index='https://pypi.python.org/simple/'):
    with urlopen(simple_index + dist + '/') as f:
        tree = ElementTree.parse(f)
    return [a.attrib['href'] for a in tree.iter('a')]

def get_JSON(omitList, json_template = r'''http://pypi.python.org/pypi/%s/json'''):
    #Ref: http://stackoverflow.com/questions/12965203/how-to-get-json-from-webpage-into-python-script
    distros = get_distributions()
    for index, dist in enumerate(distros):
        try:
            response = urlopen(json_template % dist)
        except Exception as e:
            print(dist + " likely not found")
            omitList.append(dist)
            continue
        #Unfortunate, but Python3 required hack:
        #http://stackoverflow.com/questions/6862770/python-3-let-json-object-accept-bytes-or-let-urlopen-output-strings
        str_response = response.read().decode('utf-8')
        json_data = json.loads(str_response)

        yield json_data

def getDateTime(timeStampStr):
    return datetime.strptime(timeStampStr, "%Y-%m-%dT%H:%M:%S")

#Returns a dictionary.
#{
    #'package':package,
    #'version':version,
    #'depdendencies':[
        #(package,version)    
    #]
#}
def download(package, version):
    #Run pip...
    pass

#Run has to be run first. No duh!
def run():	
    p = PackageDL()
    p.create_tables()
    #print(p.check_db('test','1.2'))
    omitList = []
    count = 0
    for data in get_JSON(omitList):
        try:
            if not data['urls'] or not data['releases']:
                omitList.append(data['info']['name'])
                continue
            if len(data['releases']) == 1:
                if not next( iter(data['releases'].values())): 
                    omitList.append(data['info']['name'])
                    continue
            p.write_to_index(data)
            count += 1
        except Exception as e:
            with open('failure_log.js', 'w') as j: json.dump(data, j)
            raise e
    p.conn.close()
    with open('omitted.js', 'w') as j: json.dump(omitList, j)
    with open('omitted_versions.js','w') as j: json.dump(p.versions_omitted, j)
    print('Done. Hopefully.')
    print(count)
run()
