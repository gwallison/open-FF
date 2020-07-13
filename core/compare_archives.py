"""
This module is used to detect silent changes in the raw data from 
one download data to the next.  The two changes we look for are simple:
Has an event in the older download been removed in the later download?
Are events in the two download identical?
When these silent changes are detected, the events are saved to an output
file for later analysis.

5/23/2020 - After spending a good bit of time on this, I am putting it aside.
There is still a lot of sleuthing  necessary to make a routine that does
a comprehensive compare that doesn't take hours.  I'm dropping it now
because I can't honestly say that anyone would ever use the results.

A few alternatives:  Run the comprehensive comparison as a remote process
in the basement. Instead of a comprehensive comparison, just do a few summary
variables: water volume, %Job.  Wait, I think that the sum check included here
could be used to do some of that??? -gwa

"""

import pandas as pd
import core.Construct_set as const_set


bulk_fn = 'testData'
bulk_fn_old = 'lastTestData'
bulk_fn = 'testData'
bulk_fn_old = 'ff_archive_2020-03-20'

processFromScratch = False
make_output_files = False

def run_compare():
    t = const_set.Construct_set(fromScratch=processFromScratch,
                                zfilename=bulk_fn,
                                make_files=make_output_files).get_full_set()
    t_old = const_set.Construct_set(fromScratch=processFromScratch,
                                zfilename=bulk_fn_old,
                                make_files=make_output_files).get_full_set()
    
    # !!!!!!!!!!!!!Need to not run compares on SkyTruth - too much time
    
# =============================================================================
#     t_loc = t.get_df_location()
#     to_loc = t_old.get_df_location()
#     t_up = t_loc.APINumber.unique()
#     to_loc['not_in_t'] = ~to_loc.APINumber.isin(t_up)
#     print(f'Number of disappeared APINumbers: {len(to_loc[to_loc.not_in_t])}')
# 
#     t_ev = t.tables['event'].get_df()
#     to_ev = t_old.tables['event'].get_df()
#     to_oper = t_old.tables['operator'].get_df()
#     to_ev = pd.merge(to_ev,to_oper,on='iOperatorName')
#     
#     t_up = list(t_ev.UploadKey.unique())
#     to_up = list(to_ev.UploadKey.unique())
#     to_ev['not_in_t'] = ~to_ev.UploadKey.isin(t_up)
#     print(f'Number of disappeared UploadKeys: {len(to_ev[to_ev.not_in_t])}')
#     lst = to_ev[to_ev.not_in_t].OperatorName.unique()
#     print(f'Operating companies that disappeared UploadKeys: {lst}')
#     
# =============================================================================
    t_all = t.get_df_cas(keepcodes='',removecodes='',event_fields=[])
    to_all = t_old.get_df_cas(keepcodes='',removecodes='',event_fields=[])
    t_all = t_all[t_all.data_source=='bulk']    
    to_all = to_all[to_all.data_source=='bulk']
    t_up = pd.DataFrame({'upk':t_all.UploadKey.unique()})
    to_up = pd.DataFrame({'upk':to_all.UploadKey.unique()})
    mg = pd.merge(t_up,to_up,how='left',indicator=True)
    cnd = mg['_merge']=='both'
    only_t = list(mg[~cnd].upk.unique())
    print(len(t_all))
    t_all = t_all[~(t_all.UploadKey.isin(only_t))]
    print(len(t_all))

    # remove generated columns
    lst = list(t_all.columns)
    lc = []
    for c in lst:
        if c[0].islower():
            lc.append(c)
    t_all = t_all.drop(lc,axis=1)
    to_all = to_all.drop(lc,axis=1)
    print('Finding overlapping events')
    print(f't_all shape: {t_all.shape},  to_all shape: {to_all.shape}')
    gbt = t_all.groupby('UploadKey',as_index=False).sum()
    gbt.to_csv('./tmp/temp1.csv')
    gbto = to_all.groupby('UploadKey',as_index=False).sum()
    gbto.to_csv('./tmp/temp0.csv')
    print(f' {len(gbt)}, {len(gbto)} {gbto.head()}')
    tmp = pd.merge(gbt,gbto,on='UploadKey',how='outer',indicator=True)
    cnd = tmp['_merge']=='both'
    tmp.to_csv('./tmp/temp.csv')
    upkeys = list(tmp[~cnd].UploadKey.unique())
    print(f'upkeys: {len(upkeys)}')
    #print(f'Number of overlapping events with problems: {len(upkeys)}')
    
    mg = pd.merge(t_all[t_all.UploadKey.isin(upkeys)],
                  to_all[to_all.UploadKey.isin(upkeys)],
                  how='outer',indicator=True)
# =============================================================================
#     mg = pd.merge(t_all,to_all,
#                   how='outer',indicator=True)
# =============================================================================
    cnd = mg['_merge']=='both'
    mg[~cnd].to_csv('./tmp/temp_final.csv')
"""
# =============================================================================
#     cnts=0
#     for uk in upkeys:
#         x = t_all[t_all.UploadKey==uk]
#         y = to_all[to_all.UploadKey==uk]
#         cmp = x.merge(y,how='outer',indicator=True)
#         test = list(cmp['_merge'].unique())==['both']
#         print(f'Are they the same? {test} ')
#         if test==False:
#             cmp.to_csv('./tmp/temp.csv')    
#             cnts += 1
#     print(f'Total events that have been silently changed: {cnts}')
# """
# =============================================================================

"""
mg = pd.merge(to_loc,t_loc,on='UploadKey',how='inner')
mg['differ'] = mg.Latitude_x - mg.Latitude_y
print(f'Number of events with differing Latitude: {len(mg[mg.differ>0])}')
print(f'Mean difference : {mg[mg.differ>0].differ.mean()}')


mg = pd.merge(to_loc,t_loc,on='UploadKey',how='inner')
mg['differ'] = mg.TotalBaseWaterVolume_x - mg.TotalBaseWaterVolume_y
print(f'Number of events with differing number of records: {len(mg[mg.differ>0])}')
print(f'Mean difference : {mg[mg.differ>0].differ.mean()}')


to_all = t_old.get_df_cas(keepcodes="",removecodes="")
t_all = t.get_df_cas(keepcodes="",removecodes="")


def get_total_hfjob(df):
    return df.groupby('UploadKey',as_index=False)['PercentHFJob'].sum()

gb1 = get_total_hfjob(to_all)
gb2 = get_total_hfjob(t_all)
mg = pd.merge(gb1,gb2,on='UploadKey',how='inner')
mg['differ'] = mg.PercentHFJob_x-mg.PercentHFJob_y
print(f'Number of events with differing HFJob totals: {len(mg[mg.differ>0])}')
print(f'Mean difference : {mg[mg.differ>0].differ.mean()}')

cnts = pd.cut(mg[mg.differ>0].differ,[0,0.0001,0.01,.1,1,10,100,1000,10000])
print(pd.value_counts(cnts))


def get_num_recs(df):
    return df.groupby('UploadKey',as_index=False)['reckey'].count()

gb1 = get_num_recs(to_all)
gb2 = get_num_recs(t_all)
mg = pd.merge(gb1,gb2,on='UploadKey',how='inner')
mg['differ'] = mg.reckey_x-mg.reckey_y
print(f'Number of events with differing number of records: {len(mg[mg.differ>0])}')
print(f'Mean difference : {mg[mg.differ>0].differ.mean()}')

mg[mg.differ>0]


upk = mg[mg.differ>0].UploadKey.tolist()
to_all[to_all.UploadKey.isin(upk)].groupby('OperatorName')['bgCAS'].count()


#from pandas.util.testing import assert_frame_equal
import pandas as pd
fields = ['reckey','UploadKey','APINumber','bgCAS']
upk = to_all.loc[700000].UploadKey
print(f'UploadKey = {upk}')
cn = t_all[t_all.UploadKey==upk].copy()
cnmin = cn.reckey.min()
cn['reckey'] = cn.reckey-cnmin
co = to_all[to_all.UploadKey==upk].copy()
comin = co.reckey.min()
co['reckey'] = co.reckey-comin
cn = cn.sort_values(fields).reset_index(drop=True)
co = co.sort_values(fields).reset_index(drop=True)
print(f'len old {len(cn)}, len new {len(cn)}')
# assert_frame_equal(cn,co)
mg = pd.merge(cn,co,on='reckey')
mg.reindex(sorted(mg.columns), axis=1)

"""