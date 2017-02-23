#Creates a collected csv like thing
import sqlite3
def run():
    con = sqlite3.connect('pypi_filtered_again.db')
    cur = con.cursor()
    cur.execute('select package,version,date,downloads from PACKAGE_INDEX order by date asc')
    packages = cur.fetchall()
    output=[]
    for tup in packages:
        package = tup[0]
        version = tup[1]
        
        cur.execute('select dependency, dependency_version from DEPENDENCIES where package=? and version=?' , (package,version)  )
        deps = cur.fetchall()
        #print(deps)
        if not deps:
            #print(package)
            #print(version)
            output.append( '%s,%s,%s,%s,,\n' % tup)
        else:
            for dep_tup in deps:
                version = dep_tup[1].replace(',',';')
                if '#' in version:
                    version = version.split('#')[0]
                version = version.strip()
                new_tup = (dep_tup[0], version)
                output.append( '%s,%s,%s,%s,%s,%s\n' % (tup + new_tup) )
    with open('allDeps.csv', 'w') as f:
        f.write(''.join(output))
    con.close()
run()