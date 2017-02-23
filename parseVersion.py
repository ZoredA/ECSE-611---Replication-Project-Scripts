import sqlite3
import re
reg = re.compile('[^0-9]+')

con = sqlite3.connect('pypi_filtered.db')
cur = con.cursor()
cur.execute('select package,version,date,downloads from PACKAGE_INDEX')
packages = cur.fetchall()

def funky(x):
    k = 0.5
    return 1 - (1 / (k * x + 1))

fixed = []
others = []
for tup in packages:
    version = tup[1]
    ver_l = version.split('.')
    if len(ver_l) < 3:
        others.append(tup)
        continue
    ver_l_1 = [reg.sub('.',x).rstrip('.').lstrip('.') for x in ver_l]
    bad = False
    for a in ver_l_1:
        if not a or a == '.':
            others.append(tup)
            bad = True
    if bad:
        continue
    try:
        ver_l = [int(x.split('.')[0]) for x in ver_l_1]
    except Exception:
        print(ver_l)
        print(version)
        raise
    p = funky(ver_l[2])
    n = funky(ver_l[1] + p)
    m = ver_l[0] + n
    if m > 1900:
        others.append(tup) #it is likely a date.
        continue
    new_ver = ( str(m) )
    #print(version)
    #print(ver_l)
    str_free = '.'.join( [ reg.sub('',x) for x in version.split('.') ][0:3] )
    new_tup = (tup[0],str_free, new_ver,tup[2],tup[3])
    fixed.append(new_tup)
    

    
with open('fixed2.csv','w') as f:
    f.write('package,version_string_removed,normalized_version,date,downloads\n')
    for tup in fixed:
        new_tup = [str(x) for x in tup]
        f.write(','.join(new_tup) + '\n')

with open('other.csv','w') as f:
    for tup in others:
        new_tup = [str(x) for x in tup]
        f.write(','.join(new_tup) + '\n')