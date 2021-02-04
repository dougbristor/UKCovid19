# https://isaric4c.net/reports/
# Age, sex and status at 28 days after admission
# Dynamic ISARIC4C / CO-CIN report to SAGE and NERVTAG

import requests

import lxml.html
       
import csv

import json
import pandas as pd 


import unicodedata
import re



import os.path
#import timestring

from datetime import datetime





def getTable(id, html, head, date, gender) :
	
	cols =  root.xpath('//*[@id="'+id+'"]/table//th')
	
	data = []
		
	chead = [col.text_content().replace('\n','') for col in cols[1:]  ]
	
	chead[0] = 'Age'
	#//data.append(chead)
	
	
	
	rows =  root.xpath('//*[@id="'+id+'"]/table//tr')
	
	
	for i, row in enumerate(rows[1:]  ):
		cols =  row.xpath('td')
		cout = [re.sub(r'\(.*\)','' , col.text_content().replace('\n' ,'') ).strip()  for col in cols[1:] ]
		data.append(cout)
	
	
	if len(head) == 0:
		head = chead
		
	
	
	#df = pd.DataFrame(data, columns = chead)
	
	

#	i#f len(head) > 0:	
#		df =  df 
		
	df = pd.DataFrame(data, columns = chead)[ head ]
	
	
	df['date'] = date	
	df['gender'] = gender		
	return df
		
	
	
	
	
	
	



FILEPATH = 'data/daily/'



url = 'https://argoshare.is.ed.ac.uk/cocin_public/00_public_figures.html'

r = requests.get(url)
root = lxml.html.fromstring(r.content)



getdate =  root.xpath('//html/body/div//div')[2]		
datestr = ":".join([col.text_content().replace('\n','') for col in getdate  ][0].split(":")[1:])[1:-1] 
	


#print( timestring.Date(datestr) )
#04 February 2021, 02:00:10

datetime_object = datetime.strptime( datestr , "%d %B %Y, %I:%M:%S")


date = datetime_object.strftime(  "%Y-%m-%d" ) 



heads = ['Age' , 'Discharged' , 'On-going care' , 'Died' , 'Total']


#df = []

id = "table-1c.-female."

gender = "female"
df_female = getTable(id, root, heads, date,  gender)



#df['female'] = df_temp 


id = "table-1b.-male."
gender = "male"
df_male = getTable(id, root, heads , date,  gender)




id = "table-1a.-all-patients."

gender = "all"
df_patients = getTable(id, root, heads , date, gender)


df = []

df = df_female.append([df_male , df_patients]) 



if os.path.isfile(FILEPATH+"patients_status.tsv")  :
	csv_params = dict(sep="\t", mode="a+",   header=False, index=False, quoting=csv.QUOTE_NONE) 

else :
	csv_params = dict(sep="\t", mode="w",  index=False, quoting=csv.QUOTE_NONE) 





df.to_csv(FILEPATH+"patients_status.tsv", **csv_params) 		
		
print("added :", df.shape[0], "rows")

exit(0)
