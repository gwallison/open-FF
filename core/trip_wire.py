# -*- coding: utf-8 -*-
"""

Created on Thu May 21 09:55:29 2020

@author: Gary
"""
import pandas as pd
import core.Find_silent_change as fsc
import core.Read_FF as rff
import shutil
#import difflib

#import core.Construct_set as const_set

output = './out/'
tempdir = './tmp/'
sources = './sources/'
tripInput = sources+'trip_wire_input.csv'

# =============================================================================
# upload_diff_ref = output+'upload_diff_ref.csv'
# change_log = output+'silent_change_log.csv'
# silent_detail = output+'silent_detail.txt'
# exclude_files = ['archive_2018_08_28.zip','sky_truth_final.zip']
# skyfn = 'sky_truth_final'
# 
# =============================================================================

def fetch_input(fn=tripInput):
    return pd.read_csv(fn)

def backup_testData(infn='testData.zip', outfn='testData_last.zip',
                    sources=sources):
    shutil.copyfile(sources+infn,sources+outfn) 


def getDfForCompare(fn,sources='./sources/'):
    fn = sources+fn
    raw_df = rff.Read_FF(zname=fn).import_raw()
    raw_df = raw_df[~(raw_df.IngredientKey.isna())]
    raw_df = raw_df.drop(['raw_filename','data_source','record_flags'],
                     axis=1)

    return raw_df

    

def showDifference(uploadlst,olddf, df):
    outstr = ''
    for uk in uploadlst:
        outstr += f'  Differences in {uk}\n'
        if fsc.compareFrameAsStrings(olddf[olddf.UploadKey==uk],
                                     df[df.UploadKey==uk]):

            conc = pd.merge(olddf[olddf.UploadKey==uk],df[df.UploadKey==uk],on='IngredientKey',how='outer',
                            indicator=True)
            cols = df.columns.tolist()
            cols.remove('IngredientKey')
            for col in cols:
                x = col+'_x'
                y = col+'_y'
                conc['comp'] = conc[x]==conc[y]
                if conc.comp.sum()<len(conc):
                    outstr += f'{conc[~conc.comp][[x,y]]}\n'
                    outstr += f'{col}, sum = {conc.comp.sum()}\n'
    if len(outstr)>0:
        print(f'  Details available at {silent_detail}')
        with open(silent_detail,'w') as f:
            f.write(outstr)

def add_to_uploadRef(rec_df):
    try:
        diff_ref = pd.read_csv(upload_diff_ref)
    except:
        diff_ref = pd.DataFrame()
    diff_ref = pd.concat([rec_df,diff_ref],sort=True)
    diff_ref.to_csv(upload_diff_ref,index=False)

def add_to_change_log(clog):
    try:
        logdf = pd.read_csv(change_log)
    except:
        logdf = pd.DataFrame()
    logdf = pd.concat([clog,logdf])
    logdf.to_csv(change_log,index=False)
    
    
    
def runTripWire(newfn,oldfn):
    
    print("Fetching raw verison of today's data set for tripwire")
    df = getDfForCompare(newfn)
    print("Fetching raw verison of previous data set for tripwire")
    olddf = getDfForCompare(oldfn)

    tripdf = fetch_input()
    for row in tripdf.itertuples(index=False):
        fieldname = row.field
        fieldval = str(row.value)
        new = df[df[fieldname]==fieldval].copy()
        old = olddf[olddf[fieldname]==fieldval].copy()
        print(f'For {fieldname} = {fieldval}, lens = {len(old)},{len(new)}')
        new = fsc.getNormalizedDF(new)
        old = fsc.getNormalizedDF(old)
        print(f'For {fieldname} = {fieldval}, lens = {len(old)},{len(new)}')


# =============================================================================
#     for i,arc in enumerate(archives[:]):        
#         print(f'\nProcessing archive for silent changes:\n    {arc}\n')
#         olddf = df.copy()
#         df = getDfForCompare(arc[1],sources=arcdir)
#         if len(olddf)==0:  # first run, nothing left to do
#             continue
#         oldulk = olddf.UploadKey.unique().tolist()
# 
#         df = fsc.getNormalizedDF(df)
#         olddf = fsc.getNormalizedDF(olddf)
#         
#         ulk = df.UploadKey.unique().tolist()
#         ukMissingFromNew = []
#         for uk in oldulk:
#             if uk not in ulk:
#                 ukMissingFromNew.append(uk)
#         #print(olddf.columns)
#         print(f'   Number of UploadKeys gone missing in new set: {len(ukMissingFromNew)}')
#         if len(ukMissingFromNew)>0:
#             tmp = olddf[olddf.UploadKey.isin(ukMissingFromNew)][['UploadKey','IngredientKey']]
#             #print(tmp.UploadKey.head())
#             tmp['ref_fn'] = archives[i-1][1]
#             tmp['new_fn'] = archives[i][1]
#             tmp['reason'] = 'UploadKey missing from newer archive'
#             add_to_uploadRef(tmp)
#             tmp = tmp.groupby('UploadKey',as_index=False).first()
#             tmp = tmp.drop('IngredientKey',axis=1)
#             tmp['ref_date'] = archives[i-1][0]
#             tmp['new_date'] = archives[i][0]
#             gb = olddf[olddf.UploadKey.isin(ukMissingFromNew)].groupby('UploadKey',as_index=True)\
#                 [['APINumber','OperatorName','JobEndDate']].first()
#             tmp = pd.merge(tmp,gb,on='UploadKey')
#             add_to_change_log(tmp)
#         
#         
#         # find matching records
#         mg = pd.merge(olddf,df,on=['UploadKey','IngredientKey'],how='outer',
#                       indicator=True,validate='1:1')
#         common = mg[mg['_merge']=='both'][['UploadKey','IngredientKey']].copy()    
#         newmg = pd.merge(common,df,on=['UploadKey','IngredientKey'],how='inner')
#         #print(newmg.columns)
#         newmg['rhash'] = pd.util.hash_pandas_object(newmg,hash_key='1234').astype('int64')    
#         oldmg = pd.merge(common,olddf,on=['UploadKey','IngredientKey'],how='inner')
#         #print(oldmg.columns)
#         oldmg['rhash'] = pd.util.hash_pandas_object(oldmg,hash_key='1234').astype('int64')   
#         pd.concat([newmg.head(),oldmg.head()]).to_csv('./tmp/temp.csv')
#         print('   Merging old/new with hash values for each row')
#         hashmg = pd.merge(oldmg[['UploadKey','IngredientKey','rhash']],
#                       newmg[['UploadKey','IngredientKey','rhash']],
#                       on=['UploadKey','IngredientKey'],validate='1:1')
#         hashmg['rdiff'] = hashmg.rhash_x != hashmg.rhash_y
#         ulk_diff_list = hashmg[hashmg.rdiff].UploadKey.unique().tolist()
# 
# 
#         print(f'   Number of rows with differing hash values: {hashmg.rdiff.sum()} out of {len(hashmg)}')
#         print(f'     in {len(ulk_diff_list)} disclosure(s)\n')
#         showDifference(ulk_diff_list,olddf,df)
#         tmp = hashmg[hashmg.rdiff][['UploadKey','IngredientKey']]
#         mgulk = hashmg.UploadKey.unique().tolist()
#         tmp['ref_fn'] = archives[i-1][1]
#         tmp['new_fn'] = archives[i][1]
#         tmp['reason'] = 'differing row hash'
#         add_to_uploadRef(tmp)
#         tmp = tmp.groupby('UploadKey',as_index=False).first()
#         tmp = tmp.drop(['IngredientKey'],axis=1)
#         tmp['ref_date'] = archives[i-1][0]
#         tmp['new_date'] = archives[i][0]
#         gb = olddf[olddf.UploadKey.isin(mgulk)].groupby('UploadKey',as_index=True)\
#             [['APINumber','OperatorName','JobEndDate']].first()
#         tmp = pd.merge(tmp,gb,on='UploadKey')
#         add_to_change_log(tmp)
#         
#         sub1 = olddf[olddf.UploadKey.isin(common.UploadKey.unique().tolist())]
#         sub1 = sub1[~(sub1.IngredientKey.isin(common.IngredientKey.unique().tolist()))]
#         print(f'\n    Number of lines in old but removed in new: {len(sub1)}')
# 
#         sub2 = df[df.UploadKey.isin(common.UploadKey.unique().tolist())]
#         sub2 = sub2[~(sub2.IngredientKey.isin(common.IngredientKey.unique().tolist()))]
#         print(f'\n    Number of lines in new but not present in old: {len(sub2)}')
# 
#     return 
# 
# =============================================================================

if __name__ == '__main__':
    #print(fsc.createInitialCompareList())
# =============================================================================
    runTripWire(None,None)
   