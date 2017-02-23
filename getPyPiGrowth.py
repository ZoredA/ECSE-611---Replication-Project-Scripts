
#Go through date range
# set baseline, all packages before sept 01
# 
#So, Sept-Oct 01 (by created date)
#So, Oct 01-Nov 01 
# etc
# get number of new, 
# number of updated (if package exists in set, it is updated

from datetime import datetime
from dateutil.relativedelta import relativedelta
import dateutil.parser
import sqlite3

class PyPiGrowth:
    def __init__(self):
        self.conn = sqlite3.connect('pypi.db')
        self.c = self.conn.cursor()
        self.current_month = None
        self.existing_set = set() #contains all the packages we've encountered before.
        self.map = {} #maps date to new packages made, and packages updated
    
    def get_baseline(self, month):
        q = "Select * from PACKAGE_INDEX where date < ? group by package"
        self.c.execute(q, (month,))
        return self.c.fetchall()
    
    
    def get_difference(self, start_date, end_date):
        q = "Select * from PACKAGE_INDEX where date > ? and date < ? group by package"
        self.c.execute(q, (start_date, end_date))
        return self.c.fetchall()
        
    def tabulate_month(self, data):
        #Go through the tups.
        new = 0
        updated = 0
        for tup in data:
            package = tup[0]
            if package in self.existing_set:
                updated += 1
            else:
                new += 1
                self.existing_set.add(package)
        return {
            'new':new,
            'updated':updated
        }

    def get_max_date(self):
        q = "SELECT max(date) from PACKAGE_INDEX"
        self.c.execute(q)
        return  dateutil.parser.parse(self.c.fetchone()[0])
        
    def run(self):
        d = datetime.strptime('2005-04-01', '%Y-%m-%d')
        end_date = self.get_max_date()
        base_line = self.get_baseline(d)
        self.existing_set = set([x[0] for x in base_line])
        current_date = d
        one_month = relativedelta(months=+1)
        all_data = []
        
        current_year = d.year
        current_month = d.month
        current_year_list = []
        
        while current_date < end_date:
            second_date = current_date + one_month
            data = self.get_difference(current_date, second_date)
            new_update_dict = self.tabulate_month(data)
            
            #current_year_dict[current_month] = new_update_dict
            current_year_list.append( (current_month, new_update_dict) )
            
            current_month = second_date.month
            if second_date.year != current_year:
                all_data.append((current_year, current_year_list))
                current_year = second_date.year
                current_year_list = []
                print('Changing to year %s' % current_year)
            current_date = second_date
        if all_data[-1] [0] != current_date.year:
            all_data.append((current_year, current_year_list))
            
        self.conn.close()
        self.write_csv(all_data)
        return all_data
        
    
    def write_csv(self, data):
        with open('growth2.csv', 'w') as f:
            f.write('year,month,new,updated\n')
            for year_tup in data:
                year = year_tup[0]
                year_list = year_tup[1]
                for tup in year_list:
                    month = tup[0]
                    updated = tup[1]['updated']
                    new = tup[1]['new']
                    line = "%s,%s-%s,%s,%s\n" % ( year, month,year, new, updated )
                    f.write(line)
            
        