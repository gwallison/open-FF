# -*- coding: utf-8 -*-
"""

Created on Thu May 21 09:55:29 2020

@author: Gary
"""
import pandas as pd
import numpy as np
import core.Find_silent_change as fsc
import core.Read_FF as rff
import shutil
import difflib
import os
import datetime
now = datetime.datetime.now()
today = datetime.datetime.today()

#import core.Construct_set as const_set

output = './out/'
tripdir = './out/tripwire_log/'
diffdir = tripdir+'diff_files/'
tempdir = './tmp/'
sources = './sources/'
arcdir = './archive/'
tripInput = sources+'trip_wire_input.csv'

#upload_diff_ref = output+'upload_diff_ref.csv'
exclude_files = ['archive_2018_08_28.zip','sky_truth_final.zip','desktop.ini']
skyfn = 'sky_truth_final'

metacols = ['APINumber','UploadKey','Latitude','TotalBaseWaterVolume',
            'CountyName','CountyNumber','FederalWell','IndianWell',
            'JobEndDate','JobStartDate','Latitude','Longitude',
            'OperatorName','StateName','StateNumber','TVD',
            'TotalBaseNonWaterVolume','WellName']

def fetch_input(fn=tripInput):
    return pd.read_csv(fn,quotechar='$',dtype={'APINumber':'str'})

def backup_testData(infn='testData.zip', outfn='testData_last.zip',
                    sources=sources):
    shutil.copyfile(sources+infn,sources+outfn) 


def getDfForCompare(fn,sources='./sources/'):
    fn = sources+fn
    raw_df = rff.Read_FF(zname=fn).import_raw() # (na_filter=False)
    raw_df = raw_df[~(raw_df.IngredientKey.isna())]
    raw_df = raw_df.drop(['raw_filename','data_source','record_flags',
                          'ClaimantCompany', 'DTMOD','IngredientComment',
                          'IngredientMSDS','IsWater','PurposeIngredientMSDS',
                          'PurposeKey','PurposePercentHFJob','Source',
                          'SystemApproach','FFVersion','DisclosureKey',
                          'Projection'],
                     axis=1)
    #raw_df['date'] = pd.to_datetime(raw_df.JobEndDate)
    return raw_df

def getDfForCompare_basic(fn,sources='./sources/'):
    fn = sources+fn
    raw_df = rff.Read_FF(zname=fn).import_raw_as_str() # (na_filter=False)
    raw_df = raw_df[~(raw_df.IngredientKey=='')]
    #print(raw_df.info())
    return raw_df
    

def showDifference(uploadlst,olddf, df):
    outstr = ''
    cols_affected = set()
    for uk in uploadlst:
        if fsc.compareFrameAsStrings(olddf[olddf.UploadKey==uk],
                                     df[df.UploadKey==uk]):
            outstr += f'  Differences in {olddf.APINumber.iloc[0]} :: {uk} \n'
            outstr += '---------------------------------------\n'

            conc = pd.merge(olddf[olddf.UploadKey==uk],df[df.UploadKey==uk],on='IngredientKey',how='outer',
                            indicator=True)
            cols = df.columns.tolist()
            cols.remove('IngredientKey')
            for col in cols:
                x = col+'_x'
                y = col+'_y'
                conc['comp'] = conc[x]==conc[y]
                if conc.comp.sum()<len(conc):
                    cols_affected.add(col)
                    if col in metacols:
                        outstr += f'{conc[~conc.comp][[x,y]].iloc[0]}\n'
                        #outstr += f'{col}, sum = {conc.comp.sum()}\n'
                        
                    else:                        
                        outstr += f'{conc[~conc.comp][[x,y]]}\n'
                        #outstr += f'{col}, sum = {conc.comp.sum()}\n'
                    outstr += '---------------------------------------\n'
    l = list(cols_affected)
    sumtxt = ''
    for c in l:
        sumtxt += c+'; '
    return outstr, sumtxt

# =============================================================================
# def compareByClosestMatch(olddf,df):
#     oldstrs = fsc.getNormalizedStrLst(olddf)
#     newstrs = fsc.getNormalizedStrLst(df)
#     print(oldstrs[0:1])
#     
# 
# =============================================================================
def get_blank_record(cols,meta):
    rec = {}
    for m in meta:
        rec[m] = False
    for col in cols:
        if not col in meta:
            rec[col] = 0
    return rec

def compileBasicDifference(olddf,newdf,outfn='unknown'):

    logtxt = '\n*************  New Disclosures Added **************\n'
    gbnew = newdf.groupby(['APINumber','UploadKey'],as_index=False)['CASNumber'].count().reset_index(drop=True)
    gbold = olddf.groupby(['APINumber','UploadKey'],as_index=False)['CASNumber'].count().reset_index(drop=True)
    gbnew = gbnew.drop('CASNumber',axis=1)
    gbold = gbold.drop('CASNumber',axis=1)
    mg = pd.merge(gbold,gbnew,on=['APINumber','UploadKey'],
                  how='outer',indicator=True)


    outdf = pd.DataFrame({'APINumber':[],
                          'UploadKey':[],
                          'new_date':[],
                          'type_of_diff':[],
                          'fields_changed':[]})
    ### New disclosures - 
    print(f' Finding unique new disclosures')
    for row in mg[mg._merge=='right_only'].itertuples(index=False):
        rec = pd.DataFrame({'APINumber':row.APINumber,
                            'UploadKey':row.UploadKey,
                            'new_date':outfn, # date of the new archive
                            'type_of_diff':'new_only',
                            'fields_changed':''},index=[0])
        outdf = pd.concat([outdf,rec],ignore_index=True,sort=True)
    logtxt += f'\n {len(mg[mg._merge=="right_only"])} new disclosures added.\n'

    ### Dropped disclosures        
    logtxt = '\n*************  Old Disclosures Dropped **************\n'
    print(f' Finding dropped disclosures')
    dropped = []
    for row in mg[mg._merge=='left_only'].itertuples(index=False):
        rec = pd.DataFrame({'APINumber':row.APINumber,
                            'UploadKey':row.UploadKey,
                            'new_date':outfn, # date of the new archive
                            'type_of_diff':'old_only',
                            'fields_changed':''},index=[0])
        outdf = pd.concat([outdf,rec],ignore_index=True,sort=True)
        # save the disclosures
        dropped.append((row.APINumber,row.UploadKey))
        cond = (olddf.APINumber==row.APINumber)&(olddf.UploadKey==row.UploadKey)
        fn = diffdir+row.APINumber+'_dropped_'+outfn+'.csv'
        olddf[cond].to_csv(fn)
    logtxt += f'\n {len(mg[mg._merge=="left_only"])} old disclosures dropped.\n'
    for rec in dropped:
        logtxt += f' -- {rec[0]}, {rec[1]}\n\n'
    

    ### Changed disclosures
    logtxt += '\n*************  Disclosures Changed **************\n'    
    cols = newdf.columns.tolist()
    cols.remove('IngredientKey')
    #records = {}
    print('   -- merge old and new for finding differences')
    # remove records where API/upK are not in both
    olddf = pd.merge(olddf,mg[~(mg._merge=='left_only')][['APINumber','UploadKey']],
                              on=['APINumber','UploadKey'],
                     how='inner')
    newdf = pd.merge(newdf,mg[~(mg._merge=='right_only')][['APINumber','UploadKey']],
                              on=['APINumber','UploadKey'],
                     how='inner')

#    conc = pd.merge(olddf,newdf,on='IngredientKey',how='outer',indicator=True)
    conc = pd.merge(olddf,newdf,how='outer',indicator=True)
    apis = conc[conc._merge!='both'].APINumber.unique().tolist()
    print(f'   -- number affected APIs: {len(apis)}')
    for api in apis:
        logtxt += 50*'*'+f'\n            << {api} >>\n'+50*'*'+'\n\n'
        t = conc[conc.APINumber==api].copy()
        logtxt += f'Shared IngredientKeys: {len(t[t._merge=="both"])}\n'
        logtxt += f'             Old only: {len(t[t._merge=="left_only"])}\n'
        logtxt += f'             New only: {len(t[t._merge=="right_only"])}\n\n'
        fn = diffdir+api+'_mixed_'+outfn+'.csv'
        t.to_csv(fn)
        #make the summary text
        told = olddf[olddf.APINumber==api].copy()
        tnew = newdf[newdf.APINumber==api].copy()
        lst = told.UploadKey.unique().tolist()
        sumtxt,dftxt = showDifference(lst,told,tnew)
        logtxt += sumtxt
        rec = pd.DataFrame({'APINumber':api,
                            'UploadKey':' -- ',
                            'new_date':outfn, # date of the new archive
                            'type_of_diff':'changes within disclosure',
                            'fields_changed':dftxt},index=[0])
        outdf = pd.concat([outdf,rec],ignore_index=True,sort=True)

#    df = pd.DataFrame.from_dict(records,orient='index')
#    return df
# =============================================================================
#         print(f'      differences in {col}: {conc.comp.sum()} in {numUp} unique UploadKeys')
#         if x == 'TotalBaseWaterVolume_x':
#             print(conc[conc.comp][['TotalBaseWaterVolume_x',
#                                    'TotalBaseWaterVolume_y',
#                                    'APINumber_x']].head(50))
# 
# =============================================================================
# =============================================================================
#                 if conc.comp.sum()<len(conc):
#                     outstr += f'{conc[~conc.comp][[x,y]]}\n'
#                     outstr += f'{col}, sum = {conc.comp.sum()}\n'
#     return outstr
# =============================================================================
    return outdf, logtxt
    
def hash_compare(df1,df2):
    new = fsc.getNormalizedDF(df1)
    old = fsc.getNormalizedDF(df2)
    new_hash = fsc.makeHashTable(new)
    old_hash = fsc.makeHashTable(old)
    return fsc.compareHashTables(old_hash,new_hash)

def basic_hash_compare(df1,df2):
    print('   -- remove new disclosures from newest download')
    print(f'      starting: {len(df1)}')
    justUpK = pd.DataFrame({'UploadKey':list(df2.UploadKey.unique())})
    df1 = pd.merge(justUpK,df1,on='UploadKey',how='left')
    print(f'      ending:   {len(df1)}')
    
    print('   -- normalizing data frames')
    new = fsc.getNormalizedBasic(df1)
    old = fsc.getNormalizedBasic(df2)
    print('   -- generating hashes')
    new = fsc.addBasicHash(new)
    old = fsc.addBasicHash(old)
    print('   -- comparing hases by ingredientKey')
    
    return fsc.compareHashBasic(old,new)
    
    
def runTripWire(newfn,oldfn,sources='./sources/',usedate='today'):
    print("Fetching raw string verison of today's data set for tripwire")
    df = getDfForCompare_basic(newfn,sources)
    print("Fetching raw string verison of previous data set for tripwire")
    olddf = getDfForCompare_basic(oldfn,sources)

    # logtxt is for human readable report
    if usedate == 'today':
        outfn = now.strftime("%Y-%m-%d")
    else:
        outfn = usedate
    logtxt = f'Tripline log created: {now}\n'
    logtxt += f'Input archives: older: {oldfn} (= x, left) \n'
    logtxt += f'Input archives: newer: {newfn} (= y, right)\n\n'


    ### First look for any differences between old and new and record pointer
# =============================================================================
#     print('   -- remove new disclosures from newest download')
#     print(f'      starting: {len(df)}')
#     justUpK = pd.DataFrame({'UploadKey':list(olddf.UploadKey.unique())})
#     df = pd.merge(justUpK,df,on='UploadKey',how='left')
#     print(f'      ending:   {len(df)}\n')
#     print(f'\n\n - detecting differences across entire old and new dataframes')
# =============================================================================
    outdf,comptxt = compileBasicDifference(olddf,df,outfn)
    outdf = outdf.reset_index()
    outdf = outdf[['APINumber','UploadKey','new_date',
                   'type_of_diff','fields_changed']]
    outdf.to_csv(tripdir+outfn+'.csv')


    # Flag any changes (including new disclosures) for API in the 
    #  hand-curated list.
    tripdf = fetch_input()
    outAPI = outdf.APINumber.unique().tolist()
    #print(outAPI)
    tt = ''
    for row in tripdf.itertuples(index=False):
        #print(f'Checking for trip check: {row.APINumber}')
        if row.APINumber in outAPI:
           tt += '*'*66+'\n'
           tt += '*'*66+'\n'
           tt += f'*********** TRIP WIRE DETECTION OF API {row.APINumber} ************\n'
           tt += f'            -- {row.note}, {row.date_added}\n'
           tt += f'\n{outdf[outdf.APINumber==row.APINumber].head(3).T}\n'
           tt += '*'*66+'\n'
           tt += '*'*66+'\n\n'
    if tt!='':
        print(tt)
        logtxt = tt + '\n' + logtxt
        
    with open(tripdir+outfn+'.txt','w') as f:
        f.write(logtxt+comptxt)

# =============================================================================
#     cols = list(outdf.columns)
#     cols.remove('index')
#     for row in outdf.itertuples(index=False):
#         logtxt += f'\n\nAPINumber : {row.index}, '
#         line = '\n  changed: '
#         for col in cols:
#             if eval(f'row.{col}>0'):
#                 line += col + ' '
#         logtxt +=  line
#         # make the specific diff_files
#         new = df[df.APINumber==row.index].copy()
#         old = olddf[olddf.APINumber==row.index].copy()
#         lst = list(new.UploadKey.unique())        
#         showdiff = showDifference(lst,old,new)
#         try:
#             apitxt = int(row.index)
#             apitxt = str(apitxt)
#         except:
#             apitxt = 'nan'
#         fn = diffdir+apitxt+'_'+outfn+'.txt'
#         txt = f'Differences found for api={row.index}\n'
#         txt += f'Input archives: older: {oldfn} (= x) \n'
#         txt += f'Input archives: newer: {newfn} (= y)\n\n'
#         txt += showdiff
#         with open(fn,'w') as f:
#             f.write(txt)
#        
# 
#     ###  Now look for targeted trip wires as alerts
#     print(f'Checking for targeted APINumbers in found silent changes')
#     for row in tripdf.itertuples(index=False):
#         api = str(row.APINumber)
#         if api in list(outdf['index'].unique()):
#             new = df[df.APINumber==api].copy()
#             old = olddf[olddf.APINumber==api].copy()
#             lst = list(new.UploadKey.unique())        
#             showdiff = showDifference(lst,old,new)
#             print(f'\n\n*** Trip wire detection for APINumber = {api} ***')
#             print(f'              Issue: {row.note}; {row.date_added}')
# 
#             logtxt += f'\n\n*** Detected differences for APINumber = {api} ***\n'
#             logtxt += f'              Issue: {row.note}; {row.date_added} \n'
#             logtxt += showdiff
# 
# 
# 
#     with open(tripdir+outfn+'.txt','w') as f:
#         f.write(logtxt)
#         
# 
# =============================================================================
def singleCompare(newfn,oldfn,apis=['37007205130000']):
    print("Fetching raw verison of today's data set for tripwire")
    df = getDfForCompare(newfn)
    print("Fetching raw verison of previous data set for tripwire")
    olddf = getDfForCompare(oldfn)

    ###  First look for targeted trip wires
    logtxt = f'Single API comparisons for {today}\n\n'
    for api in apis:
        print(f'Checking {api}')
        new = df[df.APINumber==api].copy()
        old = olddf[olddf.APINumber==api].copy()
        lst = list(new.UploadKey.unique())        
        print(f'\n\n*** Trip wire detection for APINumber = {api} ***')
        print(showDifference(lst,old,new))
        logtxt += f'\n*** Trip wire detection for APINumber = {api} ***\n'
        outstr, sumtxt = showDifference(lst,old,new)
        logtxt += outstr + '\n'
        logtxt += sumtxt + '\n\n\n'
    with open(tripdir+'single_compare.txt','w') as f:
        f.write(logtxt)

def compareByClosest(newfn,oldfn,apis=['37007205130000'],dropcols=[]):
    print("Fetching raw verison of today's data set for tripwire")
    df = getDfForCompare(newfn)
    print("Fetching raw verison of previous data set for tripwire")
    olddf = getDfForCompare(oldfn)
    d = difflib.Differ()
    ###  First look for targeted trip wires
    #logtxt = f'Single API comparisons for {today}\n\n'
    for api in apis:
        print(f'Checking {api}')
        new = df[df.APINumber==api].copy()
        old = olddf[olddf.APINumber==api].copy()
        print(f'\n\n*** Trip wire detection for APINumber = {api} ***')
        if dropcols:
            new = new.drop(dropcols,axis=1)
            old = old.drop(dropcols,axis=1)
        oldstrs = fsc.getNormalizedStrLst(old)
        newstrs = fsc.getNormalizedStrLst(new)
        for l in newstrs:
            res = difflib.get_close_matches(l,oldstrs,n=1,cutoff=0)
            #print(d.compare(l,res[0]))
            print(res)

# =============================================================================
#         print(showDifference(lst,old,new))
#         logtxt += f'\n*** Trip wire detection for APINumber = {api} ***\n'
#         outstr, sumtxt = showDifference(lst,old,new)
#         logtxt += outstr + '\n'
#         logtxt += sumtxt + '\n\n\n'
#     with open(tripdir+'single_compare.txt','w') as f:
#         f.write(logtxt)
# 
# =============================================================================
def process_archive(fnindex=0,lastindex=None):
    filelst = os.listdir(arcdir)
    for ex in exclude_files:
        filelst.remove(ex)
    cntr = fnindex
    if lastindex==None:
        lastindex = len(filelst)-1
    while cntr<lastindex:
        tdate = filelst[cntr+1][11:-4]
        print(tdate)
        runTripWire(filelst[cntr+1],
                    filelst[cntr],
                    sources=arcdir,
                    usedate=tdate)    
        cntr += 1
if __name__ == '__main__':
    #print(fsc.createInitialCompareList())
# =============================================================================
    runTripWire(None,None)
   