from typing import Iterable, Dict, Union, List
from json import dumps
from requests import get

import certifi
from http import HTTPStatus

import json
import pandas as pd 
import os.path

import csv


import urllib3
urllib3.disable_warnings()




""" Pull data from PHE and process data to store case data by age a tsv file  
Created by Doug Bristor 2020


Based on examples availble from 
PHE API visit https://coronavirus.data.gov.uk/developers-guide


Cases for Enagland & Wales
Hospital admissions : England only

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
LatestType = Iterable[str]
APIResponseType = Union[List[StructureType], str]


def get_paginated_dataset(filters: FiltersType, structure: StructureType,  latestby : LatestType = None, 
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
    
    if (latestby is None):
        api_params = {
            "filters": str.join(";", filters),
            "structure": dumps(structure, separators=(",", ":")),
            
            "format": "json" if not as_csv else "csv"
        }
    else :
        print(latestby["latestBy"])

        api_params = {
            "filters": str.join(";", filters),
            "structure": dumps(structure, separators=(",", ":")),
            "latestBy": latestby["latestBy"],
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




# expands ages in json string to columns
def expandages(data, columnname) :    
    
    table = None
    idx = 0
    for m in data[columnname]:
       
        df = pd.json_normalize(m )[ ['age','value'] ] 
        
        if table is None:
              table = pd.pivot_table(df,  columns = 'age')   
        else :
              table = table.append( pd.pivot_table(df,  columns = 'age') ) 
        
    table = table.rename_axis('index',axis=1).reset_index() 
    table[['code' ,'date'] ] = data[[ 'code','date']]
    table['gender'] = columnname;
    
    cols = list(table.columns)
    cols = cols[-3:] + cols[:-3]
    table = table[cols]
    table = table.rename_axis('index',axis=1).reset_index() 
    table.drop(['index', 'level_0'], axis=1, inplace=True)
    
    return table




# join age groups into bands
def simpleages(data, infotype , gender) : 
	
    #date =[  "2020-10-08", "2020-10-09" , '2020-10-10']
    date = [ max(data.loc[ 0: , 'date'])]    
    newdata =  data[['code', 'date']].copy()
    #gender = data.loc[ 0 , 'gender']
    #if 'female
    #infotype if 
    newdata['type'] = infotype
    newdata['gender'] = gender
    
    
    newdata['0_to_5'] =    data['0_to_4'] + ( data['5_to_9']/5).astype(int)    
    newdata['6_to_17'] =  ( data  ['5_to_9']*4/5).astype(int)  +  data['10_to_14'] +  ( data['15_to_19']*3/5).astype(int)     
    newdata['18_to_64']  =   (data['15_to_19']*2/5).astype(int)  +  data.loc[: ,  ['20_to_24', '25_to_29', '30_to_34', '35_to_39','40_to_44', '45_to_49', '50_to_54', '55_to_59', '60_to_64']].sum(axis=1)
    newdata['65_to_84'] =    data.loc[: ,  ['65_to_69', '70_to_74', '75_to_79', '80_to_84'] ].sum(axis=1)
    newdata['85+'] =   data.loc[: ,  ['85_to_89','90+'] ].sum(axis=1)
    
    # reduce areas to NHS regions
    for d in date :
        newdata['date'] = pd.to_datetime(newdata['date'])
        subdata = newdata.loc[ newdata['date'] == d]     
    
        # Midlands
        index = subdata[ (subdata['code'] == "E12000004") | (subdata['code'] == "E12000005") ].index.tolist()
        areadf = subdata.loc[index].sum(axis=0).T
        areadf['code'] = "E40000008" 
        areadf['date'] = d
        areadf['type'] = infotype
        areadf['gender'] = gender
        newdata = newdata.append(areadf , ignore_index=True).rename_axis('index',axis=1).reset_index() 
        newdata.drop(['index'], axis=1, inplace=True)
        
        # North East Yorkshire
        index = subdata[ (subdata['code'] == "E12000001") | (subdata['code'] == "E12000003") ].index.tolist()
        areadf = subdata.loc[index].sum(axis=0).to_dict()
        areadf['code'] = "E40000009" 
        areadf['date'] = d
        areadf['type'] = infotype
        areadf['gender'] = gender
        newdata = newdata.append(areadf , ignore_index=True).rename_axis('index',axis=1).reset_index() 
        newdata.drop(['index'], axis=1, inplace=True)
    
    index = newdata[  ( newdata['code'] =="E12000001") | ( newdata['code'] == "E12000003") | ( newdata['code'] == "E12000004")  |   ( newdata['code'] == "E12000005" ) ].index.tolist()
    newdata = newdata.drop(index)
        
    # reorder columns
    newdata['date'] = pd.to_datetime(newdata['date'])   
    col =[ 'code', 'date', 'type' ,'gender', '0_to_5', '6_to_17', '18_to_64', '65_to_84', '85+' ] 
     
    newdata = newdata[col].sort_values(by=['date']).reset_index().drop(['index'], axis=1)
    newdata.index.name='id'
   
    return newdata
    




if __name__ == "__main__":
	
    FILEPATH = "./data/daily/"
	
	
    query_filters = [
        f"areaType=region", 
    ]
    query_filters1 = [
        f"areaType=nation", 
    #    f"date=2020-07-20",
    ]
    query_filters2 = [
        f"areaType=nhsRegion",   
     #   f"date=2020-07-20",
    ]




    query_structure = {
        "date": "date",
        "name": "areaName",
        "code": "areaCode",
        "maleCases": "maleCases",
        "femaleCases": "femaleCases",
			
    }
    
    query_structure1 = {
        "date": "date",
        "name": "areaName",
        "code": "areaCode",
        "newAdmissions" : "newAdmissions",
        "covidOccupiedMVBeds" : "covidOccupiedMVBeds", 
        "hospitalCases": "hospitalCases", 
        }
    
    query_structure2 = {
        "date": "date",
        "name": "areaName",
        "code": "areaCode",
        "cumAdmissionsByAge": "cumAdmissionsByAge",
        }
    

    newdata = pd.concat([ pd.json_normalize(get_paginated_dataset( query_filters , query_structure)) , pd.json_normalize( get_paginated_dataset(query_filters1 , query_structure))]   , ignore_index='true' )
    newdata.index.name='id'
    
    admissionAge = pd.json_normalize(get_paginated_dataset( query_filters2 , query_structure2 , {"latestBy": "cumAdmissionsByAge" } ) )
    

    col =[ 'code', 'date','type',  'gender', '0_to_5', '6_to_17', '18_to_64', '65_to_84', '85+' ]  
    admissionAgeTable = expandages(admissionAge, 'cumAdmissionsByAge' )
    admissionAgeTable['gender'] = 'people'
    admissionAgeTable['type'] = 'admission'   
    admissionAgeTable = admissionAgeTable[col]
    
      
    maletable = expandages(newdata, 'maleCases' )
    femaletable = expandages(newdata, 'femaleCases' )
    newmale = simpleages(maletable, 'TotalPositive', 'Male' )
    newfemale = simpleages(femaletable, 'TotalPositive' , 'Female')
    
    ## write if files don't exist
    
    csv_params_write = dict(sep="\t", index=False, quoting=csv.QUOTE_NONE )


    if not os.path.isfile(FILEPATH+"maleCases.tsv")  :
        csv_params = dict(sep="\t", index=False, quoting=csv.QUOTE_NONE )

        maletable.to_csv(FILEPATH+"maleCases.tsv", sep="\t",**csv_params)   
        femaletable.to_csv(FILEPATH+"femaleCases.tsv", **csv_params) 
        
        admissionAgeTable.to_csv(FILEPATH+"admissionAge.tsv",**csv_params) 
        maletable.to_csv(FILEPATH+"SummaryMaleCases.tsv", **csv_params)   
        femaletable.to_csv(FILEPATH+"SummaryFemaleCases.tsv", **csv_params)    
        
        # merged summary age cases table by date
        newmale.to_csv(FILEPATH+"cases_latest.tsv", **csv_params)        
        newfemale.to_csv(FILEPATH+"cases_latest.tsv", **csv_params)     
        admissionAgeTable.to_csv(FILEPATH+"cases_latest.tsv", **csv_params)      
        
        
        
    else :    
        readtsv = pd.read_csv(FILEPATH+"maleCases.tsv", sep="\t",  quoting=csv.QUOTE_NONE).tail(5)  
        
        
        print( "Latest Date from data:" ,  max(readtsv['date'])  )
        print( "Latest Date from file:",  pd.to_datetime(max(maletable['date']) ) )
        
        """ check for latest update new data   """
        if pd.to_datetime(max(readtsv['date'])) ==  pd.to_datetime(max(maletable['date'])) : 
             print ("CSV already latest date")
		
		
        else :
			
			
             welshtable = pd.read_csv(FILEPATH+"maleCases.tsv", sep="\t",  quoting=csv.QUOTE_NONE)
             maletable = pd.concat([maletable,  welshtable[ welshtable[0]=='W92000004' ]])
             
             welshtable = pd.read_csv(FILEPATH+"femaleCases.tsv", sep="\t",  quoting=csv.QUOTE_NONE)
             femaletable = pd.concat([femaletable,  welshtable[ welshtable[0]=='W92000004' ]])
             
             
             welshtable = pd.read_csv(FILEPATH+"SummaryMaleCases.tsv", sep="\t",  quoting=csv.QUOTE_NONE)
             newmale = pd.concat([newmale,  welshtable[ welshtable[0]=='W92000004'] ])
             
             welshtable = pd.read_csv(FILEPATH+"SummaryFemaleCases.tsv", sep="\t",  quoting=csv.QUOTE_NONE)
             newfemale = pd.concat([newfemale,  welshtable[ welshtable[0]=='W92000004'] ])
                          
             csv_params = dict(sep="\t", mode="a+", header=False, index=False, quoting=csv.QUOTE_NONE) # date_format='%Y-%m-%d'

             # raw age cases table by date
             maletable.to_csv(FILEPATH+"maleCases.tsv", **csv_params_write)   
             femaletable.to_csv(FILEPATH+"femaleCases.tsv",  **csv_params_write)  
		
		     # summary age cases table by date
             newmale.to_csv(FILEPATH+"SummaryMaleCases.tsv", **csv_params_write)   
             newfemale.to_csv(FILEPATH+"SummaryFemaleCases.tsv", **csv_params_write)  
             admissionAgeTable.to_csv(FILEPATH+"admissionAge.tsv", **csv_params) 
     
             # merged summary age cases table by date
             #newmale.to_csv(FILEPATH+"cases_latest.tsv", **csv_params)        
             #newfemale.to_csv(FILEPATH+"cases_latest.tsv", **csv_params)     
             #admissionAgeTable.to_csv(FILEPATH+"cases_latest.tsv", **csv_params)  
             print ("Files updated")
                
        
            
    exit(0)
