# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison


Change the file handles at the top of this code to appropriate directories.

This script is used to analyze and adjust the location data of FracFocus. It results
in 4 gnerated fields (bgLatitude, bgLongitude, bgStateName, and bgCountyName) as
well as a set of flags that will go into the record_flags:
    L - general indication of a location issue or problem, set for all below
    F - Lat/lon are flipped.  bgLatitude=Longitude and vice versa
    G - lat/lon are probably insuffieciently detailed to work in well pad
          identification.  bglat/lon are the same as raw
    O - lat or lon are out of physical range - bglat/lon set to county centers
    D - lat or lon appear to be out of the recorded county.  bglat/lon are the same as raw
---
    N - State/county names don't match reference. bgStateName/County corrected 
        to proper names
    
"""
from geopy.distance import geodesic
import pandas as pd
import numpy as np
import core.Construct_set as const_set
import core.Read_FF as rff
#from pyproj import Transformer

#### -----------   File handles  -------------- ####
sources = './sources/'
upload_ref_fn = sources+'uploadKey_ref.csv'

#### ----------    end File Handles ----------  ####

# =============================================================================
# def getGoogleProjCoord(lat,lon,proj='WGS84'):
#     # returns lat, lon in the projection that Google uses (WGS84), but
#     #  you need to include the input projection
#     if proj.upper() == 'WGS84':
#         return lat,lon # no conversion needed
#     transformer = Transformer.from_crs(proj.upper(), 'WGS84')
#     olon,olat = transformer.transform(lon, lat)
#     return olat,olon
# 
# =============================================================================
def set_distance(row):
    """ used to calculate the distance between two sets of lat/lon in miles.
    If there are problems, in the calculation, an arbitrarily large number is
    returned."""
    
    if row.refLatitude<1:
        return 9999 # arbitrary big number; 
    try:
        return geodesic((row.Latitude,row.Longitude),
                        (row.refLatitude,row.refLongitude)).miles
    except: # problems? set to a big number
        print(f'geodesic exception on {row.Latitude}, {row.Longitude};; {row.refLatitude}, {row.refLongitude}')
        return 9999

def save_upload_ref(df):
    """save the data frame that serves as an uploadKey reference; in particular
    best guesses on location data as well as the date that the event was
    added to this project (as opposed to the date it first appears in FF)"""
    
    df[['UploadKey','StateName','bgStateName','CountyName','bgCountyName',
        'Latitude','bgLatitude','Longitude','bgLongitude','loc_flags',]].to_csv(upload_ref_fn,index=False)

def get_upload_ref():
    """Fetches the uploadKey reference dataframe"""
    t = pd.read_csv(upload_ref_fn,low_memory=False)
    return t[['UploadKey','bgStateName','bgCountyName',
        'bgLatitude','bgLongitude','loc_flags']].copy()

def get_decimal_len(s):
    """used to find the length of the decimal part of  lan/lon"""
    t = str(s)
    for c in t:
        if c not in '-.0123456789':
            return -1
    if '.' not in t:
        return 0
    while '.' in t:
        try:
            t = t[1:]
        except:
            pass
    return len(t)


##########  Main script ###########

######  Make list of disclosures whose lat/lons are not specific enough
df_latlon = rff.Read_FF().import_all_str(varsToKeep=['UploadKey',
                                                     'Latitude','Longitude'])
t = df_latlon.groupby('UploadKey',as_index=False)[['Latitude',
                                                   'Longitude']].first()
t['latdeclen'] = t.Latitude.map(lambda x: get_decimal_len(x))
t['londeclen'] = t.Longitude.map(lambda x: get_decimal_len(x))
t['too_coarse'] = (t.londeclen+t.latdeclen)<5

# use this list at the end of script
tooCoarse = list(t[t.too_coarse].UploadKey.unique())

##############################################################


tab_const = const_set.Construct_set(fromScratch=False).get_full_set()

df = tab_const.get_df_location(['Latitude','Longitude','Projection',
                                'StateNumber','CountyNumber',
                                'StateName','CountyName',
                                'UploadKey','iUploadKey'])
df = pd.merge(df,get_upload_ref(),on='UploadKey',how='left',validate='1:1')

# flag empty  or obviously wrong lat/lon 

df['in_range_lat'] = (df.Latitude>5)&(df.Latitude<90)
df['in_range_lon'] = (df.Longitude<-5)&(df.Longitude>-180)


#### Get reference data
ref = pd.read_csv('./sources/state_county_ref.csv')
ref = ref[['refStateName','refCountyName','refStateNumber','refCountyNumber',
           'refLatitude','refLongitude','refTotalAreaMi^2']]

# first compare lat/lon by state and coundy CODES
t = pd.merge(df,ref,left_on=['StateNumber','CountyNumber'],
             right_on=['refStateNumber','refCountyNumber'],how='left')



t['newflags'] = ''
print(f'starting distance calc on {len(t)} records')


# determine distance of point from the county's reference point
t['geo_distance'] = t.apply(lambda x: set_distance(x),axis=1)

# consider county as a circle: what is radius?
t['county_radius'] = (t['refTotalAreaMi^2']/3.14)**0.5                            
t.newflags = np.where(t.geo_distance<t.county_radius*5,
                      t.newflags,
                      t.newflags+'-D') # give wide berth (5x) to the distance before chucking

t['bgLatitude'] = np.where(t.in_range_lat,t.Latitude,t.refLatitude)
t['bgLongitude'] = np.where(t.in_range_lon,t.Longitude,t.refLongitude) 
t.newflags = np.where(t.in_range_lat&t.in_range_lon,
                       t.newflags,t.newflags+'-O')

# detect potentially swapped lat lon.
t['flipped_loc'] = (t.Longitude>5)&(t.Longitude<90)&(t.Latitude<-5)&(t.Latitude>-180)                                
t.bgLatitude = np.where(t.flipped_loc,t.bgLongitude,t.bgLatitude)
t.bgLongitude = np.where(t.flipped_loc,t.bgLatitude,t.bgLongitude)
t.newflags = np.where(t.flipped_loc,t.newflags+'-F',t.newflags)

t.bgLatitude = np.where(t.bgLatitude.isna(),t.Latitude,t.bgLatitude)
t.bgLongitude = np.where(t.bgLongitude.isna(),t.Longitude,t.bgLongitude)

# =============================================================================
# print('Starting generation of WSG Projection - **** takes a LONG time ****')
# t['tup'] = t.apply(lambda x: getGoogleProjCoord(x.bgLatitude,x.bgLongitude,
#                                                 x.Projection),axis=1)
# 
# t[['wgsLat', 'wgsLon']] = pd.DataFrame(t['tup'].tolist(), index=t.index) 
# =============================================================================
df = pd.merge(df[['Latitude', 'Longitude', 'StateNumber', 'CountyNumber', 'StateName',
                  'CountyName', 'UploadKey', 'iUploadKey']],
              t[['iUploadKey','bgLatitude','bgLongitude','newflags']],
#                 'wgsLat','wgsLon']],
              on='iUploadKey',how='left')
#print(df.columns)
df['loc_flags'] = df.newflags

######
##  Now process name checks
######

t = df.copy()
print(f'Number of records for State/County name work: {len(t)}')
# make df with unique location identifiers, and make clean names 
t['lcStateName'] = t.StateName.str.strip().str.lower()
t['lcCountyName'] = t.CountyName.str.strip().str.lower()

## bring in ref info
#ref = pd.read_csv('./sources/state_county_ref.csv')
#ref = ref[['refStateName','refCountyName','refStateNumber','refCountyNumber']]
t = pd.merge(t,ref,left_on=['StateNumber','CountyNumber'],
             right_on=['refStateNumber','refCountyNumber'],how='left')
cond = (t.lcStateName!=t.refStateName)|(t.lcCountyName!=t.refCountyName)

t['nameflags'] = np.where(cond,'-N','')
t['newStateName'] = t.refStateName # always use the ref name
t['newCountyName'] = t.refCountyName
df = pd.merge(df,t[['nameflags','newStateName','newCountyName','iUploadKey']],
              on='iUploadKey',how='left')
df['bgStateName'] = df.newStateName
df['bgCountyName'] = df.newCountyName
df.loc_flags = np.where(~df.nameflags.isna(),
                        df.loc_flags+df.nameflags,
                        df.loc_flags)
df.loc_flags = np.where(df.UploadKey.isin(tooCoarse),
                        df.loc_flags+'-G',df.loc_flags)
df.loc_flags = np.where(df.loc_flags.str.contains('G|F|O|D',regex=True),
                        df.loc_flags+'-L',df.loc_flags)

save_upload_ref(df)



