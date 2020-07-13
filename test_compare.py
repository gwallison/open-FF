# -*- coding: utf-8 -*-
"""

Created on Thu May 21 09:55:29 2020

@author: Gary
"""
import pandas as pd
import core.Find_silent_change as fsc
import core.Read_FF as rff
import difflib

#import core.Construct_set as const_set

output = './out/'
tempdir = './tmp/'
arcdir = './arc_testing/'
upload_hash_ref = output+'upload_hash_ref.csv'
change_log = output+'silent_change_log.csv'
exclude_files = ['archive_2018_08_28.zip','sky_truth_final.zip']
skyfn = 'sky_truth_final'

def getDfForCompare(fn,sources='./sources/'):
    fn = sources+fn
    raw_df = rff.Read_FF(zname=fn).import_raw()
    raw_df = raw_df[~(raw_df.IngredientKey.isna())]
    return raw_df

def initializeSilentChangeRecords():
    """Careful!!!  This does what the name describes!"""
    ref = pd.DataFrame({'UploadKey':[],'last_hash':[]})
    fsc.saveUpdatedHash(ref)
    
def startFromScratch():
    """Be aware - this initializes everything before running a LONG process on
    all archived files!"""
    initializeSilentChangeRecords()
    archives = fsc.createInitialCompareList()
    new = pd.DataFrame({'UploadKey':None,'rhash':None},index=[])
    df = pd.DataFrame()
    
    for arc in archives[:2]:        
        print(f'\nProcessing archive for silent changes:\n    {arc}\n')
        olddf = df.copy()
        old = new.copy()
        old = old.rename({'rhash':'last_hash'},axis=1)

        df = getDfForCompare(arc[1],sources=arcdir)
        df = df.fillna(-9)
        new = fsc.makeHashTable(df)
        
        #print(old.head())
        out = fsc.compareHashTables(old,new)
        print(f'Number silent changes: {out.silent_change.sum()}')

    # finding problems...
    if out.silent_change.sum()>0:
        print('Silent changes detected...')
        ukl = out[out.silent_change].UploadKey.unique().tolist()
        print(f'Number of disclosures with silent change detected: {len(ukl)}')
        #uk = out[out.silent_change].iloc[0].UploadKey
        for uk in ukl[:10]:
            if fsc.compareFrameAsStrings(olddf[olddf.UploadKey==uk],
                                         df[df.UploadKey==uk]):
    
                conc = pd.merge(olddf[olddf.UploadKey==uk],df[df.UploadKey==uk],on='IngredientKey',how='outer',
                                indicator=True)
                cols = df.columns.tolist()
                cols.remove('IngredientKey')
                #print(f'Diff UploadKey: {uk}')
                #print(f'length conc: {len(conc)}')
                for col in cols:
                    x = col+'_x'
                    y = col+'_y'
                    conc['comp'] = conc[x]==conc[y]
                    if conc.comp.sum()<len(conc):
                        print(f'{conc[~conc.comp][[x,y]]}')
                        print(f'{col}, sum = {conc.comp.sum()}')

# =============================================================================
#     conc = conc.reindex(sorted(conc.columns), axis=1)
#     conc.to_csv('./tmp/temp.csv')
# =============================================================================
    return out

def showDifference(uploadlst,olddf, df):
    for uk in uploadlst:
        print(f'  Differences in {uk}')
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
                    print(f'{conc[~conc.comp][[x,y]]}')
                    print(f'{col}, sum = {conc.comp.sum()}')
    
    

def startFromScratch2():
    """Be aware - this initializes everything before running a LONG process on
    all archived files!"""
    initializeSilentChangeRecords()
    archives = fsc.createInitialCompareList()
    #new = pd.DataFrame({'UploadKey':None,'rhash':None},index=[])
    df = pd.DataFrame()
    
    for arc in archives[-2:]:        
        print(f'\nProcessing archive for silent changes:\n    {arc}\n')
        olddf = df.copy()
        df = getDfForCompare(arc[1],sources=arcdir)
        if len(olddf)==0:  # first run, nothing left to do
            continue
        oldulk = olddf.UploadKey.unique().tolist()

        df = fsc.getNormalizedDF(df)
        olddf = fsc.getNormalizedDF(olddf)
        
        ulk = df.UploadKey.unique().tolist()
        ukMissingFromNew = []
        for uk in oldulk:
            if uk not in ulk:
                ukMissingFromNew.append(uk)
        print(f'   Number of UploadKeys gone missing in new set: {len(ukMissingFromNew)}')

        # find matching records
        mg = pd.merge(olddf,df,on=['UploadKey','IngredientKey'],how='outer',
                      indicator=True,validate='1:1')
        common = mg[mg['_merge']=='both'][['UploadKey','IngredientKey']].copy()    
        newmg = pd.merge(common,df,on=['UploadKey','IngredientKey'],how='inner')
        #print(newmg.columns)
        newmg['rhash'] = pd.util.hash_pandas_object(newmg,hash_key='1234').astype('int64')    
        oldmg = pd.merge(common,olddf,on=['UploadKey','IngredientKey'],how='inner')
        #print(oldmg.columns)
        oldmg['rhash'] = pd.util.hash_pandas_object(oldmg,hash_key='1234').astype('int64')   
        pd.concat([newmg.head(),oldmg.head()]).to_csv('./tmp/temp.csv')
        print('   Merging old/new with hash values for each row')
        mg = pd.merge(oldmg[['UploadKey','IngredientKey','rhash']],
                      newmg[['UploadKey','IngredientKey','rhash']],
                      on=['UploadKey','IngredientKey'],validate='1:1')
        mg['rdiff'] = mg.rhash_x != mg.rhash_y
        ulk_diff_list = mg[mg.rdiff].UploadKey.unique().tolist()
        print(f'   Number of rows with differing hash values: {mg.rdiff.sum()} out of {len(mg)}')
        print(f'     in {len(ulk_diff_list)} disclosure(s)\n')
        showDifference(ulk_diff_list,olddf,df)
        out = mg[mg.rdiff][['UploadKey','IngredientKey']]
        out['reason'] = 'differing hash'
        out['new_download_date'] = arc[0]
        out.to_csv('./tmp/silent_changes_detected.csv')
    return 


if __name__ == '__main__':
    #print(fsc.createInitialCompareList())
# =============================================================================
    startFromScratch2()
   