#  download and store Sweden ICU figures
import requests

#from bs4 import BeautifulSoup
import lxml.html
       
import xml.etree.ElementTree

import csv

import json
import pandas as pd 


FILEPATH = 'data/daily/'

swedish = ["Tabellen är uppdaterad:","Antal vårdtillfällen:","Antal patienter:","Ålder - medel:","Ålder - median:","Kön kvinna:","Dagar från insjuknande till IVA-vård:","Kronisk hjärt-lungsjukdom:","Kronisk lever-njursjukdom:","Diabetes:","Hypertoni:","Någon riskfaktor:","Någon riskfaktor innefattar: barn med flerfunktionshinder, hypertoni, 65 år eller äldre, graviditet, kronisk hjärt-lungsjukdom, kronisk lever-njursjukdom, nedsatt immunförsvar, diabetes, fetma, neuromuskulär sjukdom, annan angiven riskfaktor."]
english = ["The table is updated:","Number of admissions:","Number of patients:","Age - means:","Age - Median:","Gender: Female:","Days from the onset of ICU care:","Congestive heart-lung disease:","Chronic liver-kidney:","Diabetes:","hypertension:","Some risk factors:","Some risk factors include: children with multiple disabilities, hypertension, age 65 or older, pregnancy, chronic cardio-pulmonary disease, chronic liver-kidney disease, weakened immune system, diabetes, obesity, neuromuscular disease, other specified risk factor."]


url = 'https://portal.icuregswe.org/utdata/tabledata/covid'

r = requests.get(url)

#soup = BeautifulSoup(r.content, 'html.parser')
root = lxml.html.fromstring(r.content)
rows =  root.xpath('/html/body/table/tbody/tr')

data1 = []
data2 = []
data3 = []


for i, row in enumerate(rows[:-1]): 

	if len(row.text_content().strip())==0 : 
		continue
	cols = row.text_content().strip().split('\n')
	cols[1] = cols[1].replace(',', '.')
	
	
	
	cols[0] = english[i] 
	if '(' in cols[1] :
		print(cols[1])
		if len(cols)==3: 
			cols[2] = cols[1].split('(')[1][:-1]
			cols[1] = cols[1].split('(')[0]
		else :
			
			cols.append(cols[1].split('(')[1][:-1])
			cols[1] = cols[1].split('/')[0]
		
	
	if '%' in cols[1]:
		cols[0] += '(%)'
		cols[1] = cols[1].replace('%', '') 
	cols[1] = cols[1].replace(' ', '')	
	
	if i==0 :
		cols[1] = cols[1].strip()+" "+cols[2].strip()	
	
	
	data2.append(   cols[1].strip())
	if len(cols)==3: 
		data3.append(cols[2]);
	else :
		data3.append("")
	
	data1.append(cols[0])

df = pd.DataFrame(data2)

csv_params = dict(sep="\t", mode="a+",   header=False, index=False, quoting=csv.QUOTE_NONE) # date_format='%Y-%m-%d' 
df.T.to_csv(FILEPATH+'sweden_icu.tsv', **csv_params) 


df = pd.DataFrame(data1)
df['value'] = pd.DataFrame(data2)


print (df.T)

exit(0)


"""
Tabellen är uppdaterad:
Antal vårdtillfällen:
Antal patienter:
Ålder - medel:
Ålder - median:
Kön kvinna:
Dagar från insjuknande till IVA-vård:
Kronisk hjärt-lungsjukdom:
Kronisk lever-njursjukdom:
Diabetes:
Hypertoni:
Någon riskfaktor:
Någon riskfaktor innefattar: barn med flerfunktionshinder, hypertoni, 65 år eller äldre, graviditet, kronisk hjärt-lungsjukdom, kronisk lever-njursjukdom, nedsatt immunförsvar, diabetes, fetma, neuromuskulär sjukdom, annan angiven riskfaktor.		


The table is updated:
Number of admissions:
Number of patients:
Age - means:
Age - Median:
Gender: Female:
Days from the onset of ICU care:
Congestive heart-lung disease:
Chronic liver-kidney:
Diabetes:
hypertension:
Some risk factors:
Some risk factors include: children with multiple disabilities, hypertension, age 65 or older, pregnancy, chronic cardio-pulmonary disease, chronic liver-kidney disease, weakened immune system, diabetes, obesity, neuromuscular disease, other specified risk factor.


"""
