# go through date range
# get number of rows (denotes how many packages depend on our thing

from datetime import datetime
from dateutil.relativedelta import relativedelta
import dateutil.parser
import sqlite3
import pgrank

class PyPiPopularGrowth:
    def __init__(self):
        self.conn = sqlite3.connect('pypi_filtered_again.db')
        self.c = self.conn.cursor()
        self.current_month = None
        
                
    def get_within_date(self, start_date, end_date):
        
        q = """select dp.package,dp.dependency,pi.date from DEPENDENCIES dp, PACKAGE_INDEX pi
    where pi.package=dp.package and pi.version=dp.version
    and pi.date < ?
    order by dp.package"""
    
        self.c.execute(q, (end_date,) )
        return self.c.fetchall()
    
    def get_max_date(self):
        q = "SELECT max(date) from PACKAGE_INDEX"
        self.c.execute(q)
        return  dateutil.parser.parse(self.c.fetchone()[0])
    
    def write_dated_csv(self, tup_list, date_range):
        output = ['%s,%s' % (x[0],x[1]) for x in tup_list]
        output = '\n'.join(output)
        name = r"dated/%s.csv"% date_range
        with open(name, 'w') as f:
            f.write('package,dependency\n')
            f.write(output)
        return name
    #First we create all of our dated csvs...
    #Then we pass em one by one to the graph thing
    
    def run(self, packages):
        d = datetime.strptime('2011-10-01', '%Y-%m-%d')
        end_date = self.get_max_date()
        
        current_date = d
        one_month = relativedelta(months=+1)
        all_data = []
        
        current_year = d.year
        current_month = d.month
        current_year_list = []
        
        while current_date < end_date:
            second_date = current_date + one_month
            data = self.get_within_date(current_date, second_date)
            csv_name = self.write_dated_csv(data, "%s-%s" % (current_month, current_year) )
            
            #current_year_dict[current_month] = new_update_dict
            current_year_list.append( (current_month, csv_name) )
            
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
        try:
            ranked_dicts = self.get_rank_dicts(all_data,packages)
        except Exception as e:
            raise
        return ranked_dicts
    
    def get_rank_dicts(self,data, packages):
        #Runs the pg rank code
        #gets the data
        sorted_dicts = []
        for tup in data:
            year = tup[0]
            year_list = tup[1]
            year_output = []
            for month_tup in year_list:
                
                month = month_tup[0]
                input_csv_name = month_tup[1]
                output_name = "%s-%s" % (month,year)
                di = pgrank.just_get_dict(input_csv_name)
                print('Got page rank for %s' % input_csv_name)
                month_values = {y : 0 for y in packages}
                month_values.update( { y+'_rank' : 30000 for y in packages} )
                
                for index, tup in enumerate(di):
                    try:
                        if tup[0] in month_values:
                            month_values[tup[0]] = tup[1]
                            month_values[tup[0] + '_rank'] = (index+1)
                    except Exception:
                        print(tup)
                        raise
                year_output.append((output_name,month_values))
            sorted_dicts.append((year, year_output))
        return sorted_dicts
        
def make_large_csv(packages, all_data):
    #This takes in a list of 
    #[ ( year, [(month-year, values), (month-year, values),..]), ( year, [(month-year, values), (month-year, values),..])..
    #and turns it into an output csv of format:
    #year #month-year #package1, #package2, #package3,...
    #2011,10-2011,0.2,0.1,0,...
    try:
        header = "Year,Month,%s,%s" % (','.join(packages), '_rank,'.join(packages))
        lines = [header]
        for year_tup in all_data:
            year = year_tup[0]
            line = str(year) + ",{month}," + ",".join( ["{%s}" %x for x in packages] ) + "," + ",".join( ["{%s_rank}" % x for x in packages] )
            for month_tup in year_tup[1]:
                print(month_tup)
                our_dict = month_tup[1]
                our_dict['month'] = month_tup[0]
                o_line = line.format(**our_dict)
                lines.append(o_line)
        with open('output_ranks2.csv','w') as f:
            f.write('\n'.join(lines))
    except Exception as e:
        print(e)
        raise
    return (packages,all_data)

print('loaded')
        
def run():
    packages = [
        'six',
        'requests',
        'Django',
        'chardet',
        'certifi',
        'chardet2',]

    d = PyPiPopularGrowth()
    all_data = d.run(packages)
    return make_large_csv(packages,all_data)