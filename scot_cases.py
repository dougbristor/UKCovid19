


from json import dumps
from requests import get

import certifi
from http import HTTPStatus

import json
import pandas as pd 
import os.path

import csv
import urllib3




import io




FILEPATH = "./data/daily/"



#urllib3.disable_warnings()


#url = 'https://www.opendata.nhs.scot/api/3/action/datastore_search?resource_id=19646dce-d830-4ee0-a0a9-fcec79b5ac71&limit=5&q=title:jones'  

url = 'https://www.opendata.nhs.scot/datastore/dump/19646dce-d830-4ee0-a0a9-fcec79b5ac71?bom=True&format=tsv'

def get_dataset(url) :
	

	response = get(url, timeout=100) #,  verify=False)

	if response.status_code >= HTTPStatus.BAD_REQUEST:
		raise RuntimeError(f'Request failed: {response.text}')
	elif response.status_code == HTTPStatus.NO_CONTENT:
		return

	io_buf = io.BytesIO( response.content)
	io_buf.seek(0)
	readtsv = pd.read_csv(io_buf, sep="\t", index_col=0,  quoting=csv.QUOTE_NONE) 
	io_buf.close()

    # read local file
	#readtsv = pd.read_csv("scotin.tsv", sep="\t",  quoting=csv.QUOTE_NONE) 
	
	
	
	#_id	Date	location_code	location_name	geography	Sex	agegroup	new_positive	total_positive	crude_rate_positive	new_deaths	total_deaths	crude_rate_deaths	total_negative	crude_rate_negative

	
	
	newnames = ['location_code' , 'Date' , 'Sex' , 'agegroup' , 'total_positive' , 'total_deaths' , 'total_negative']
	
	renames = ['Country', 'Date','Sex' , 'AgeGroup'	,	'TotalPositive',	'TotalDeaths' , 'TotalNegative']
	
	
	
	for idx, colname in enumerate(newnames) :
		if colname in readtsv.columns: 
			readtsv[renames[idx]] = readtsv[colname]
	
	
	
	col = ['Country', 'Date','Sex' , 'AgeGroup'	,	'TotalPositive',	'TotalDeaths' , 'TotalNegative']
	
	
	
	
	
	readtsv['Date'] = pd.to_datetime(readtsv['Date'], format='%Y%m%d')
	 
	readtsv = readtsv[col]
	
	return readtsv





def reorder_ages(data) :

	genders = ['Female' , 'Male' ] 
	date = data[ 'Date'][1]
	code = data[ 'Country'][1]
	
	tableout = None
	
	for g in genders :
		
		table = data.loc[data['Sex'] ==g ].T 
		table.columns = table.loc['AgeGroup', :   ].tolist()
		
		rows = ['Date',	'Country'	,'Sex' , 'AgeGroup']
		
		table.drop(rows, axis=0, inplace=True)
		
		
		# 85plus => 85+
		
		
		cols = [ '0 to 4', '5 to 14', '15 to 19', '20 to 24', '25 to 44', '45 to 64',  '65 to 74', '75 to 84', '85+' ]
		table = table[cols]
		
		table['code'] = code
		table['date'] = pd.to_datetime(date)
		table['type'] = table.index.tolist()
		table['gender'] = g
		
		cols = list(table.columns)
		cols =  cols[-4:] + cols[:-4]				
		table = table[cols]
		table = table.reset_index(drop=True)
		
		if tableout is None:
			tableout = table[:]
		else :
			tableout = tableout.append(table)
			


		
		
		
	return tableout
	    

def  group_ages(data) :
    col = ['code', 'date' ,  'type' ,   'gender' ]
        
    newdata = data.copy()[col]   
        
    newdata['0_to_5'] =    data['0 to 4'] + ( data['5 to 14']/10).astype(int)    
    newdata['6_to_17'] =  ( data  ['5 to 14']*9/10).astype(int)  +  ( data['15 to 19']*3/5).astype(int)     
    newdata['18_to_64']  =   (data['15 to 19']*2/5).astype(int) +  data.loc[: ,  ['20 to 24',  '25 to 44', '45 to 64']].sum(axis=1).astype(int) 
    newdata['65_to_84'] =    data.loc[:, ['65 to 74', '75 to 84'] ].sum(axis=1).astype(int) 
    newdata['85+'] =   data['85+']
    
    
    
    
    
    return newdata







ds = get_dataset(url) 

ages = reorder_ages(ds)


## ckeck date of last entry  
readtsv = pd.read_csv(FILEPATH+'scot_daily.tsv', sep="\t",  quoting=csv.QUOTE_NONE).tail(5) 

print( "Latest Date from data:" ,  max(readtsv['date'])  )
print( "Latest Date from file:",  pd.to_datetime(max(ages['date']) ) )

if pd.to_datetime(max(readtsv['date'])) ==  pd.to_datetime(max(ages['date'] ) ) : 
    print ("CSV already latest date")
		
		
else :
    csv_params = dict(sep="\t", mode="a+",   header=False, index=False, quoting=csv.QUOTE_NONE) # date_format='%Y-%m-%d' 
    ages.to_csv(FILEPATH+'scot_daily.tsv', **csv_params) 

    groupages = group_ages(ages) 
#print(ages)



#print(groupages)

    groupages.to_csv(FILEPATH+'scot_group_ages.tsv', **csv_params) 

    groupages.to_csv(FILEPATH+'cases_latest.tsv', **csv_params) 
    print ("Files updated")

