# -*- coding: utf-8 -*-
"""
Created on Fri Jun 12 10:58:10 2020

@author: Gary

These are the routines used to find the differences between raw downloads
Aug 2020 The hashing method, while successful on smaller scales, never was really
a reliable way to find subtle difference between large data sets - I never really
could track down what the problem was -- hashes were often different for what seemed to 
be identical data sets.

In the end, it was fairly simple and not too time consuming to just use direct 
comparison of strings.  Much more straight forward.

"""

import pandas as pd
import os
from datetime import datetime
#import difflib
#import pprint


output = './out/'
#upload_hash_ref = output+'upload_hash_ref.csv'
exclude_files = ['archive_2018_08_28.zip','sky_truth_final.zip']



def addBasicHash(df):
    """return dataframe with IngredientKey only.  This is
    used to find if there are any differences between downloads for each
    disclosure.
    Note that the disclosure hash is insensitive to row position
    THIS IS THE FAST VERSION
    """
    df = df.fillna(-9)
    df['rhash'] = pd.util.hash_pandas_object(df).astype('int64')    
    return df[['IngredientKey','rhash']]

def makeHashTable(df):
    """return dataframe with UploadKey and hash for each disclosure.  This is
    used to find if there are any differences between downloads for each
    disclosure.
    Note that the disclosure hash is insensitive to row position
    THIS IS THE FAST VERSION
    """
    #print(f'makeHashTable df.columns {df.info()}')
    #print(f'Head: {df.head()}')
# =============================================================================
#     df = df.fillna(-9)
#     df = df.sort_values(by=['UploadKey','IngredientKey'])
#     df = df.reset_index(drop=True)
#     #print(f'Head: {df[["UploadKey","IngredientKey"]].head(50)}')
# =============================================================================
    
    df['rhash'] = pd.util.hash_pandas_object(df).astype('int64')    
    tmp = df.groupby('UploadKey',as_index=False)[['rhash']].sum()
    tmp.rhash = tmp.rhash.astype('uint64')
    gb = df.groupby('UploadKey',as_index=False)[['APINumber',
                                                 'OperatorName',
                                                 'JobEndDate']].first()
    tmp = pd.merge(tmp,gb,on='UploadKey',how='left')
    return tmp

def makeHashTable2(df):
    """return dataframe with UploadKey and hash for each disclosure.  This is
    used to find if there are any differences between downloads for each
    disclosure.
    Note that the disclosure hash is insensitive to row position
    """
    #df = df.fillna(-9999)
    #df['rhash'] = pd.util.hash_pandas_object(df).astype('int64')    
    grouped = df.groupby('UploadKey')
    hashlst = []
    upk =  []
    for name, group in grouped:
        #hashid = hash(group.to_csv().encode('utf-8'))
        hashid = group.to_csv().encode('utf-8')
        hashlst.append(hashid)
        upk.append(name)
        #print(hashid,group)
    return pd.DataFrame({'UploadKey':upk,'rhash':hashlst})

def makeHashTable3(df):
    """return dataframe with UploadKey and hash for each disclosure.  This is
    used to find if there are any differences between downloads for each
    disclosure.
    Note that the disclosure hash is insensitive to row position
    """
    #df = df.fillna(-9999)
    #df['rhash'] = pd.util.hash_pandas_object(df).astype('int64')    
    grouped = df.groupby('UploadKey')
    hashlst = []
    upk =  []
    for name, group in grouped:
        upk.append(name)
        work = group.sort_values(['UploadKey','IngredientKey'])
        work = work.reset_index(drop=True)
        work['rhash'] = pd.util.hash_pandas_object(work).astype('int64')
        sumit = work.rhash.sum().astype('uint64')
        hashlst.append(sumit)
    return pd.DataFrame({'UploadKey':upk,'rhash':hashlst})

def getNormalizedStrLst(df):
    work = df.sort_values(['IngredientKey'])
    work = work.reset_index(drop=True)
    work['rhash'] = pd.util.hash_pandas_object(work).astype('int64')
    str_tmp = work.to_csv().encode('utf-8')
    return str_tmp.splitlines(keepends=True)
 
def getNormalizedDF(df):
    work = df.fillna(-9)
    work = work[work.ingKeyPresent].sort_values(['UploadKey','IngredientKey'])
    work = work.reset_index(drop=True)
    return work

def getNormalizedBasic(df):
    work = df.fillna(-9)
    work = work.reset_index(drop=True)
    return work

def compareFrameAsStrings(df1,df2):
    lst1 = getNormalizedStrLst(df1)
    lst2 = getNormalizedStrLst(df2)
    if len(lst1)!=len(lst2):
        return True
    for i in range(len(lst1)):
        if lst1[i]!=lst2[i]:
            return True
    return False

def compareHashTables(old,new):
    old = old.rename({'rhash':'last_hash',
                      'APINumber':'lastAPI',
                      'OperatorName':'lastOperator',
                      'JobEndDate':'lastDate'},axis=1)
    mg = pd.merge(old,new,on='UploadKey',how='outer',indicator=True)
    rtn = ''
    mg['diff'] = ~(mg.last_hash==mg.rhash)
    mg['dropped_upk'] = mg['_merge']=='left_only'
    mg['new_upk'] = mg['_merge']=='right_only'
    mg['silent_change']= (mg['_merge']=='both') & (mg['diff'])
    if mg.new_upk.sum()>0:
        rtn += f'new uploadkeys: {mg.new_upk.sum()}; '
    if mg.dropped_upk.sum()>0:
        rtn += f'dropped uploadkeys: {mg.dropped_upk.sum()}; '
    if mg.silent_change.sum()>0:
        rtn += f'silent changes: {mg.silent_change.sum()}; '
    if mg['diff'].sum()>0:
        rtn += f'record diffs: {mg["diff"].sum()}; '
    cond = mg['diff'] | mg.dropped_upk | mg.new_upk | mg.silent_change
    
    return rtn,mg[cond].filter(['UploadKey','APINumber','OperatorName',
                          'JobEndDate','diff','dropped_upk','new_upk',
                          'silent_change'],axis=1)

def compareHashBasic(old,new):
    old = old.rename({'rhash':'last_hash'},axis=1)
    mg = pd.merge(old,new,on='IngredientKey',how='outer',indicator=True)
    rtn = ''
    mg['diff'] = ~(mg.last_hash==mg.rhash)
    mg['dropped_upk'] = mg['_merge']=='left_only'
    mg['new_upk'] = mg['_merge']=='right_only'
    mg['silent_change']= (mg['_merge']=='both') & (mg['diff'])
    if mg.new_upk.sum()>0:
        rtn += f'new ingKey: {mg.new_upk.sum()}; '
    if mg.dropped_upk.sum()>0:
        rtn += f'dropped ingKey: {mg.dropped_upk.sum()}; '
    if mg.silent_change.sum()>0:
        rtn += f'silent changes: {mg.silent_change.sum()}; '
    if mg['diff'].sum()>0:
        rtn += f'record diffs: {mg["diff"].sum()}; '
    cond = mg['diff'] | mg.dropped_upk | mg.new_upk | mg.silent_change
    
    return rtn,mg[cond]

def createInitialCompareList(exclude_files = ['archive_2018_08_28.zip']):
    """Returns a sorted list of all the files (expect those in "exclude") 
    Can be used to produce a baseline hashtable reference etc.  This should be 
    performed only when there have been additions to the comparing routines that you
    want to capture across all archived files."""
    
    files = os.listdir("./archive/")
    tups = []
    for file in files:
        #fn = file.split('.')[0]
        if file in exclude_files: files.remove(file) # remove the name
        else: 
            dt = datetime.fromtimestamp(os.path.getmtime("./archive/"+file))
            tups.append((dt.strftime("%Y-%m-%d"),file))
    tups.sort()
    return tups
    
if __name__ == '__main__':
    createInitialCompareList()
    