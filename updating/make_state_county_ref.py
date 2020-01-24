# -*- coding: utf-8 -*-
"""
Created on Sun Nov 10 10:15:35 2019

@author: Gary

Used to clean up and merge two data sources on State/County names and data
"""

import pandas as pd

def get_api():
    api = pd.read_csv('APIStateCountyCodesFullList.csv')
    return api

def get_wiki():
    wiki = pd.read_csv('county_data_wikipedia_MichaelJ.csv')
    # remove footnote marks
    wiki['county_origin'] = wiki.CountyName
    wiki.CountyName = wiki.CountyName.str.replace('[','')
    wiki.CountyName = wiki.CountyName.str.replace(']','')
    wiki.CountyName = wiki.CountyName.str.replace(r'[0-9]','')
    wiki.Latitude = wiki.Latitude.str.replace(r'[^0-9.]','')
    wiki.Longitude = wiki.Longitude.str.replace(r'[^0-9.]','')
    # add the negative sign back
    wiki.Longitude = '-' + wiki.Longitude.str.replace(r'[^0-9.]','')
    
    return wiki    


if __name__ == '__main__':
    api = get_api()
    wiki = get_wiki()
    print(wiki.Longitude.head())
    mg = pd.merge(wiki,api,on=['StateName','CountyName'],how='outer')