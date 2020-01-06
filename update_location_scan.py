# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison


Change the file handles at the top of this code to appropriate directories.

    
"""
from geopy.distance import geodesic
#import datetime
import pandas as pd
import numpy as np
import core.Construct_set as const_set

tab_const = const_set.Construct_set(fromScratch=False).get_full_set()


#### -----------   File handles  -------------- ####
sources = './sources/'
upload_ref_fn = sources+'uploadKey_ref.csv'

#### ----------    end File Handles ----------  ####

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
    
    df[['UploadKey','date_added','bgStateName','bgCountyName',
        'bgLatitude','bgLongitude','loc_flags',
        'google_addr_key']].to_csv(upload_ref_fn,index=False)

def get_upload_ref():
    """Fetches the uploadKey reference dataframe"""
    return pd.read_csv(upload_ref_fn,low_memory=False)


# =============================================================================
# 
# ##################################################################
# # construct upload_ref -- this performed ONE TIME to initialize!
# uploadref = tab_const.tables['event'].get_df(fields=['UploadKey'])
# uploadref['date_added'] = '2019-10-28'
# uploadref['bgStateName'] = np.NaN
# uploadref['bgCountyName'] = np.NaN
# uploadref['bgLatitude'] = np.NaN
# uploadref['bgLongitude'] = np.NaN
# uploadref['loc_flags'] = ''
# uploadref['google_addr_key'] = np.NaN
# save_upload_ref(uploadref)
# ##################################################################
# 
# =============================================================================

df = tab_const.get_df_location(['Latitude','Longitude',
                                'StateNumber','CountyNumber',
                                'UploadKey','iUploadKey'])
df = pd.merge(df,get_upload_ref(),on='UploadKey',how='left',validate='1:1')

# fill empty or obviously wrong lat/lon with numbers for later calc
df.Latitude = np.where(df.Latitude<5,1,df.Latitude)
df.Latitude = np.where(df.Latitude>90,1,df.Latitude)
df.Longitude = np.where(df.Longitude>-5,-1,df.Longitude) # all lon should be negative
df.Longitude = np.where(df.Longitude<-180,-1,df.Longitude) # all lon should be negative

ref = pd.read_csv('./sources/state_county_ref.csv')
ref = ref[['refStateName','refCountyName','refStateNumber','refCountyNumber',
           'refLatitude','refLongitude','refTotalAreaMi^2']]
# first compare lat/lon by state and coundy codes
t = pd.merge(df,ref,left_on=['StateNumber','CountyNumber'],
             right_on=['refStateNumber','refCountyNumber'],how='left')
t = t[(t.bgLatitude.isna())|(t.bgLongitude.isna())].copy()
print(f'starting distance calc on {len(t)} records')
t['geo_distance'] = t.apply(lambda x: set_distance(x),axis=1)

# consider county as a circle: what is radius?
t['county_radius'] = (t['refTotalAreaMi^2']/3.14)**0.5                            
t['latlon_ok'] = t.geo_distance<t.county_radius*5 # give wide berth (5x) to the distance before chucking

t['newLatitude'] = np.where(t.latlon_ok,t.Latitude,np.NaN) # good enough
t['newLongitude'] = np.where(t.latlon_ok,t.Longitude,np.NaN)
cond1 = (~t.latlon_ok)&(t.refLatitude==5) # no ref lat/lon available
t['newLatitude'] = np.where(cond1,t.Latitude,t.newLatitude) # take FF lat/lon
t['newLongitude'] = np.where(cond1,t.Longitude,t.newLongitude)
cond2 = (~t.latlon_ok)&(t.refLatitude>5) # FF lat/lon very different from ref
t['newLatitude'] = np.where(cond2,t.refLatitude,t.newLatitude) # take FF lat/lon
t['newLongitude'] = np.where(cond2,t.refLongitude,t.newLongitude)
t['newflags'] = np.where(t.latlon_ok,'','-L')
#t.to_csv('./tmp/temp.csv',index=False)


df = pd.merge(df,t[['iUploadKey','newLatitude','newLongitude','newflags','latlon_ok']],
              on='iUploadKey',how='left')
df.bgLatitude = np.where(~df.newLatitude.isna(),df.newLatitude,df.bgLatitude)
df.bgLongitude = np.where(~df.newLongitude.isna(),df.newLongitude,df.bgLongitude)
df.loc_flags = np.where(~df.newflags.isna(),df.newflags,df.loc_flags)
save_upload_ref(df)


#####  WORK ON STATE AND COUNTY NAMES
## remake frames
df = tab_const.get_df_location(['StateName','StateNumber',
                                'CountyName','CountyNumber',
                                'UploadKey','iUploadKey'])
df = pd.merge(df,get_upload_ref(),on='UploadKey',how='left',validate='1:1')
t = df[(df.bgStateName.isna())|(df.bgCountyName.isna())].copy()
print(f'Number of records for State/County name work: {len(t)}')
# make df with unique location identifiers, and make clean names 
t['lcStateName'] = t.StateName.str.strip().str.lower()
t['lcCountyName'] = t.CountyName.str.strip().str.lower()

# bring in ref info
ref = pd.read_csv('./sources/state_county_ref.csv')
ref = ref[['refStateName','refCountyName','refStateNumber','refCountyNumber']]
t = pd.merge(t,ref,left_on=['StateNumber','CountyNumber'],
             right_on=['refStateNumber','refCountyNumber'],how='left')
cond = (t.lcStateName!=t.refStateName)|(t.lcCountyName!=t.refCountyName)

t['nameflags'] = np.where(cond,'','-N')
t['newStateName'] = t.refStateName # always use the ref name
t['newCountyName'] = t.refCountyName
df = pd.merge(df,t[['nameflags','newStateName','newCountyName','iUploadKey']],
              on='iUploadKey',how='left')
df.bgStateName = np.where(~df.newStateName.isna(),df.newStateName,df.bgStateName)
df.bgCountyName = np.where(~df.newCountyName.isna(),df.newCountyName,df.bgCountyName)
df.loc_flags = np.where(~df.nameflags.isna(),
                        df.loc_flags+df.nameflags,
                        df.loc_flags)

save_upload_ref(df)



