from typing import Iterable, Dict, Union, List
from json import dumps
from requests import get



from http import HTTPStatus

import json
import pandas as pd 

import csv

import urllib3
urllib3.disable_warnings()



""" Pull data from PHE and  process data to create a common date for each nation and region creates a tsv file  
Created by Doug Bristor 2020


Based on examples availble from 
PHE API visit https://coronavirus.data.gov.uk/developers-guide

"""



"""Extract nested values from a JSON tree."""
def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []
    
    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            print(obj.items())
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            print(obj)
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values





StructureType = Dict[str, Union[dict, str]]
FiltersType = Iterable[str]
APIResponseType = Union[List[StructureType], str]


def get_paginated_dataset(filters: FiltersType, structure: StructureType,
                          as_csv: bool = False) -> APIResponseType:
    """
    Extracts paginated data by requesting all of the pages
    and combining the results.

    Parameters
    ----------
    filters: Iterable[str]
        API filters. See the API documentations for additional
        information.

    structure: Dict[str, Union[dict, str]]
        Structure parameter. See the API documentations for
        additional information.

    as_csv: bool
        Return the data as CSV. [default: ``False``]

    Returns
    -------
    Union[List[StructureType], str]
        Comprehensive list of dictionaries containing all the data for
        the given ``filters`` and ``structure``.
    """
    endpoint = "https://api.coronavirus.data.gov.uk/v1/data"

    api_params = {
        "filters": str.join(";", filters),
        "structure": dumps(structure, separators=(",", ":")),
        "format": "json" if not as_csv else "csv"
    }

    data = list()

    page_number = 1

    while True:
        # Adding page number to query params
        api_params["page"] = page_number
        print(endpoint,api_params)

        
        response = get(endpoint, params=api_params, timeout=100,  verify=False)

        if response.status_code >= HTTPStatus.BAD_REQUEST:
            raise RuntimeError(f'Request failed: {response.text}')
        elif response.status_code == HTTPStatus.NO_CONTENT:
            break

        if as_csv:
            csv_content = response.content.decode()

            # Removing CSV header (column names) where page 
            # number is greater than 1.
            if page_number > 1:
                data_lines = csv_content.split("\n")[1:]
                csv_content = str.join("\n", data_lines)

            data.append(csv_content.strip())
            page_number += 1
            continue

        current_data = response.json()
        page_data: List[StructureType] = current_data['data']
        
        data.extend(page_data)

        # The "next" attribute in "pagination" will be `None`
        # when we reach the end.
        if current_data["pagination"]["next"] is None:
            break

        page_number += 1

    if not as_csv:
        return data

    # Concatenating CSV pages
    return str.join("\n", data)


if __name__ == "__main__":
	
    """ possible filters  """
      #nhsRegion", #nhsRegion", ## nation", #nation", #region", #ltla", #region", #ltla",# nhsRegion", #ltla", # nation", #", #overview" #region" #,
      #  f"date=2020-07-20",	
	
	
    query_filters_A= [
        f"areaType=region",  
    ]
    
    query_filters_B = [
        f"areaType=nation", 
    ]


    """ possible data structure  """
       # "daily":  "hospitalCases", #newAdmissions", 
       # "admissionsByAge": "cumAdmissionsByAge",
       # "newCasesByPublishDate" : "newCasesByPublishDate",
       # "maleCases": "maleCases",
       # "femaleCases": "femaleCases",
       #"newPillarOneTestsByPublishDate" : "newPillarOneTestsByPublishDate",
       #"newPillarTwoTestsByPublishDate" :"newPillarTwoTestsByPublishDate",
       #"newPillarThreeTestsByPublishDate" :"newPillarThreeTestsByPublishDate",
       #"newPillarFourTestsByPublishDate": "newPillarFourTestsByPublishDate",      
       # "newTestsByPublishDate": "newTestsByPublishDate",
       #"newAdmissions" : "newAdmissions",
       #"covidOccupiedMVBeds" : "covidOccupiedMVBeds", 
       #"hospitalCases": "hospitalCases",        
       #"hospital": "hospitalCases",       
       # "male": "maleDeaths",
       #"daily":  "newDeathsByPublishDate" , #newAdmissions",  #"newCasesBySpecimenDate",
       # "cumulative": "cumDeathsByPublishDate"
       #  "cumulative": "cumAdmissions", #"cumCasesBySpecimenDate"

    query_structure_A = {
        "date": "date",
        "name": "areaName",
        "code": "areaCode",
        "newDeathsByDeathDate": "newDeaths28DaysByDeathDate" ,
        "newCasesBySpecimenDate": "newCasesBySpecimenDate", 

			
			
			
			
    }
    
    
    
    query_structure_B = {
        "date": "date",
        "name": "areaName",
        "code": "areaCode",
        "newAdmissions" : "newAdmissions",
        "covidOccupiedMVBeds" : "covidOccupiedMVBeds", 
        "hospitalCases": "hospitalCases", 
                "newPillarOneTestsByPublishDate" : "newPillarOneTestsByPublishDate"	,
        "newPillarTwoTestsByPublishDate": "newPillarTwoTestsByPublishDate",		
   		"newPillarThreeTestsByPublishDate" : "newPillarThreeTestsByPublishDate"	,
   		"newPillarFourTestsByPublishDate" : "newPillarFourTestsByPublishDate"	
    }
    
    

    
    # data = pd.json_normalize(get_paginated_dataset(query_filters, query_structure))
    

    
    
    """   Get data on Deaths and Cases  """
    data = pd.concat([ pd.json_normalize(get_paginated_dataset( [ f"areaType=region"], query_structure_A)) , pd.json_normalize( get_paginated_dataset([f"areaType=nation"] , query_structure_A))]   , ignore_index='true' )

    
    data.index.name='id'
    daily = pd.date_range(start=data['date'].min(), end=data['date'].max(), freq='D')
      
    areas = data['code'].unique()
    
    newdata = None
    
    for area in areas:
	    #print (area)
	    areadf = data.loc[ (data['code'] == area) ].set_index('date')	    
	    areadf.index = pd.DatetimeIndex(areadf.index)
	    areadf = areadf.reindex(daily, method='ffill')
	    areadf.index.name='date'
	    areadf = areadf.fillna(0).reset_index()
	    
	    
	    if newdata is None: 
    		newdata = areadf
	    else : 
    		newdata = newdata.append(areadf, ignore_index=True)
	    
    
    """ Combine West and East Midlands as Midlands"""
    areadf = newdata.loc[ (newdata['code'] == "E12000004") ]['date'].reset_index(drop=True).rename("date").to_frame()
    areadf['code'] = "E40000008"
    areadf['name'] = "Midlands" 
    
    
    """  drop data East Midlands and West Midlands  """     
    areadf['newDeathsByDeathDate'] = (newdata.loc[ (newdata['code'] == "E12000004") ]['newDeathsByDeathDate'].reset_index(drop=True) + newdata.loc[ (newdata['code'] == "E12000005") ]['newDeathsByDeathDate'].reset_index(drop=True)).to_frame()  
    areadf['newCasesBySpecimenDate'] = (newdata.loc[ (newdata['code'] == "E12000004") ]['newCasesBySpecimenDate'].reset_index(drop=True) + newdata.loc[ (newdata['code'] == "E12000005") ]['newCasesBySpecimenDate'].reset_index(drop=True)).to_frame()
    newdata = newdata.append(areadf, ignore_index=True)
    
    
    """ Combine North East & Yorkshire and Hull as  North East and Yorkshire"""
    areadf = newdata.loc[ (newdata['code'] == "E12000003") ]['date'].reset_index(drop=True).rename("date").to_frame()
    areadf['code'] = "E40000009"
    areadf['name'] = "North East and Yorkshire"
    
    
    """  drop data for North East & Yorkshire  and Hull  """   
    areadf['newDeathsByDeathDate'] = (newdata.loc[ (newdata['code'] == "E12000003") ]['newDeathsByDeathDate'].reset_index(drop=True) + newdata.loc[ (newdata['code'] == "E12000001") ]['newDeathsByDeathDate'].reset_index(drop=True)).to_frame()    
    areadf['newCasesBySpecimenDate'] = (newdata.loc[ (newdata['code'] == "E12000003") ]['newCasesBySpecimenDate'].reset_index(drop=True) + newdata.loc[ (newdata['code'] == "E12000001") ]['newCasesBySpecimenDate'].reset_index(drop=True)).to_frame()
    newdata = newdata.append(areadf, ignore_index=True)
    
   
    
    
    """   Get data on Hosptials admissions, Overnight and ventilator   """
    data = pd.concat([ pd.json_normalize(get_paginated_dataset( [ f"areaType=nhsRegion"], query_structure_B)) , pd.json_normalize( get_paginated_dataset([f"areaType=nation"] , query_structure_B))]   , ignore_index='true' )
    data.index.name='id'
    areas = data['name'].unique()
    
    newmergedata = None
    
    
    """ table data by unifided timedate  """
    for area in areas:
	    #print (area)	
	    areadf = data.loc[ (data['name'] == area) ].set_index('date')	    
	    areadf.index = pd.DatetimeIndex(areadf.index)
	    
	    areadf = areadf.reindex(daily, method='ffill')
	    areadf.index.name='date'
	    areadf = areadf.fillna(0).reset_index()
	    
	    areadf['newDeathsByDeathDate']  = newdata.loc[ (newdata['name'] == area) ]['newDeathsByDeathDate'].reset_index(drop=True).to_frame()  
	    areadf['newCasesBySpecimenDate']  = newdata.loc[ (newdata['name'] == area) ]['newCasesBySpecimenDate'].reset_index(drop=True).to_frame() 
	    if newmergedata is None: 
    		newmergedata = areadf
	    else : 
    		newmergedata = newmergedata.append(areadf, ignore_index=True)	    
	
	
	# add column name to index
    newmergedata.index.name='id'
    
    
    #print(newmergedata)
    filename = "./data/daily/latest.tsv"
    # save to tsv file
    newmergedata.to_csv(filename, sep="\t", quoting=csv.QUOTE_NONE)    

    print ("created TSV file '"+filename+"' \nDate range "+ min(newmergedata['date']).strftime('%B %d, %Y') +" to "+ max(newmergedata['date']).strftime('%B %d, %Y') +"\ntable size", newmergedata.shape )  
     
	 
