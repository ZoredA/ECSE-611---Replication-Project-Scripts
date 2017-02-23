import requests
import json
import sqlite3

def getClassifiers():
    con = sqlite3.connect('pypi_filtered.db')
    cur = con.cursor()
    try:
        q = """select t1.package,t1.version, t1.newest_date as newest_date, dl_sum as downloads from 

            (select package,version, max(date)  as newest_date from PACKAGE_INDEX
            group by package
            order by package desc) t1
            INNER JOIN 

            (select sum(downloads) as dl_sum , package from PACKAGE_INDEX
                group by package
                    order by dl_sum desc ) t2
                    ON t1.package=t2.package
            Order by downloads desc 
            """
        cur.execute(q)
        package_list = [x[0] for x in cur.fetchall()]
        con.close()
        print("select query done")
        con = sqlite3.connect('classifiers.db')
        cur = con.cursor()
        q = "INSERT INTO classifiers (Packages, Classifiers) values (?,?)"
        count = 0
        attempts = 0
        for package in package_list:
            if check_db(cur, package):
                count += 1
                continue
            try:
                data = requests.get('http://pypi.python.org/pypi/%s/json' % package)
            except Exception as e:
                attempts += 1
                if attempts > 3:
                    raise
                else:
                    continue
            attempts = 0
            try:
                js = data.json()
            except json.JSONDecodeError as err:
                print(err)
                classifiers = []
            try:
                classifiers = js['info']['classifiers']
            except KeyError as e:
                print("no classifiers found for %s" % package)
                classifiers = None 
            class_str = json.dumps(classifiers)
            cur.execute(q, (package, class_str) )
            count +=1
            if count % 10 == 0:
                print(count)
            if count % 100 == 0:
                print('committing')
                con.commit()
    except Exception as e:
        con.close()
        raise
    con.commit()
    con.close()
    
def check_db(cur, package):
    q = "select * from classifiers where Packages=?"
    cur.execute(q, (package,))
    if cur.fetchone():
        return True
    return False