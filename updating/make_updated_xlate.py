# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 08:50:57 2019

@author: Gary

This code is used to create new versions of the xlate files that include
any NEW codes that were not in the previous data set.  These files
can then be hand curated to fully process the new dataset.
"""
import pandas as pd
import csv
import core.Categorize_records as cat_rec
import core.CAS_tools as ct


tmpdir = './tmp/'
indir = './sources/'
single_xlate_set = [] # currently NO one-to-one xlate files in use

# in this shared_xlate, the key is the name of the xlate table and the
# items in the list are the sources for this hybrid xlate list.
shared_xlate = {'company':[('supplier','Supplier'), # (tableName,fieldName)
                           ('operator','OperatorName')]}

def check_for_new_cas(tab_manager=None):
    # Make a list of CAS numbers to run through SciFinder

    np = pd.DataFrame({'not_perf':cat_rec.Categorize_CAS(tab_manager=tab_manager).get_corrected_not_perf()})
    np['keep'] = np.not_perf.map(lambda x : ct.is_valid_CAS_code(x))

    np[np.keep].to_csv(tmpdir+'cas_to_check.csv',index=False)
    

def gen_new_files(tab_manager=None):
    """NOTE: THIS MAY CLOBBER ANY FILES NAMED ABOVE AND REPLACE THEM
    WITH CLEAN COPIES.  PAY ATTENTION!"""
    
# =============================================================================
#     # first, the fields that have a unique translation table
#     for field in single_xlate_set:
#         xlate = pd.read_csv(indir+field+'_xlate.csv',
#                             quotechar='$')
#         original = list(xlate.original)
#         lst = list(df[field].str.lower().str.strip().unique())
#         
#         print(f'{field}:  Len original: {len(original)}, new {len(lst)} ')
#         for new in lst:
#             if new not in original:
#                 print(f'adding <{new}> to {field}')
#                 df2 = pd.DataFrame({'primary':['?'],
#                                     'original':[new],
#                                     'status':['new']})
#                 xlate = xlate.append(pd.DataFrame(df2),sort=True)
#                 
#         xlate.to_csv(outdir+field+'_xlateNEW.csv',
#                      quotechar='$',quoting=csv.QUOTE_ALL,index=False)
#         
# =============================================================================

    # next the fields that share a translation table
    for shared in shared_xlate.keys():
        xlate = pd.read_csv(indir+shared+'_xlate.csv',
                            quotechar='$')
        original = list(xlate.original)
        masterset = set()
        for tup in shared_xlate[shared]:
            tableName = tup[0]
            fieldName = tup[1]
            df = tab_manager.tables[tableName].get_df()
            lst = list(df[fieldName].str.lower().str.strip().unique())
            for x in lst:
                masterset.add(x)
        for new in masterset:
            if new not in original:
                print(f'adding <{new}> to {shared} from {fieldName}')
                df2 = pd.DataFrame({'primary':['?'],
                                    'original':[new],
                                    'status':['new']})
                xlate = xlate.append(pd.DataFrame(df2),sort=True)
                
        xlate.to_csv(tmpdir+shared+'_xlateNEW.csv',
                     quotechar='$',quoting=csv.QUOTE_ALL,index=False)
                
                
    # Now update the CASNumber file cas_labels
    cas_old = pd.read_csv(indir+'cas_labels.csv',quotechar='"')
    cas_old.drop_duplicates(subset='clean',keep='first',inplace=True)
    #print(cas_old.clean.unique())
    
    df = tab_manager.tables['cas'].get_df(['CASNumber','iCASNumber'])
    df['cas_strip'] = df.CASNumber.str.lower().str.strip()
    df = pd.merge(tab_manager.tables['allrec'].get_df(fields=['iCASNumber','iUploadKey']),
                  df, on='iCASNumber',how='left')
    gb = df.groupby('cas_strip',as_index=False)['iUploadKey'].count()
    gb.columns = ['clean','new_cnt']
    #print(gb.clean.unique())
    new_df = pd.merge(cas_old,gb,
                      on=['clean'],how='outer',validate='m:1')
    new_df.to_csv(tmpdir+'cas_labelsNEW.csv',index=False)
    

