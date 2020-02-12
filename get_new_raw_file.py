
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 24 14:43:54 2019

@author: Gary
This script is used to download a new raw set, save it if the 
day of the week is in the list, and look for new events.
It will also record how many records are in each new event.

This script runs independently of the main build_database set.  It is designed 
to run autonomously, usually executed from a crontab command.
"""

import core.Construct_set as const_set
import pandas as pd
import requests
import subprocess
import shutil
from datetime import datetime

do_download = True

today = datetime.today()
if today.weekday() in [0]: # Monday= 0, Sunday = 6
    archive_file=True
else:
    archive_file=False

# define    
sources = './sources/'
archive = './archive/'
datefn= './out/upload_dates.csv'
skyfn = 'sky_truth_final'
afile = archive+f'ff_archive_{today.strftime("%Y-%m-%d")}.zip'
currfn = 'testData'
outdir = './out/'
tempfolder = './tmp/'
#webworkfolder = './website_gen/'

# =============================================================================
# # for nicer displays of numbers: round to significant figures.
# from math import log10, floor
# def round_sig(x, sig=2):
#     if abs(x)>=1:
#         out =  int(round(x, sig-int(floor(log10(abs(x))))-1))
#         return f"{out:,d}" # does the right thing with commas
#     else: # fractional numbers
#         return str(round(x, sig-int(floor(log10(abs(x))))-1))
#         
# =============================================================================

# get and save files
if do_download:
    url = 'http://fracfocusdata.org/digitaldownload/fracfocuscsv.zip'
    print(f'Downloading data from {url}')
    r = requests.get(url, allow_redirects=True)
    print('Download completed')
    open(sources+currfn+'.zip', 'wb').write(r.content)  # overwrites last file.
    if archive_file: open(afile, 'wb').write(r.content)

## Now process file
print(f'Working on data set')
outdf = pd.read_csv(datefn)
uklst = outdf.UploadKey.unique()
t = const_set.Construct_set(fromScratch=True,
                            zfilename=currfn,
                            sources=sources,
                            outdir=outdir,
                            tempfolder=tempfolder,
                            stfilename=skyfn).get_quick_set()
df = t.get_df_cas(keepcodes='',removecodes='')
df = df[~df.UploadKey.isin(uklst)] # just the new ones
gb = df.groupby('UploadKey',as_index=False)['iUploadKey'].count()
gb['date_added'] = today.strftime("%Y-%m-%d")
gb.rename({'iUploadKey':'num_records'}, inplace=True,axis=1)
print(f'New events: {len(gb)}')

outdf = pd.concat([outdf,gb],sort=True)
outdf.to_csv(datefn,index=False)
t.pickleAll() # pickle so notebook has access to data.

s= 'jupyter nbconvert --template=nbextensions --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.timeout=-1 --execute daily_report.ipynb --to=html '
print(subprocess.run(s))
shutil.copyfile('daily_report.html',
                'c:/Users/Gary/Google Drive/webshare/daily_report.html')

# =============================================================================
# print(df.columns)
# # show where today's fracks are
# if (len(gb)!=0): 
#     locat = t.get_df_location()[['UploadKey','StateName','CountyName',
#                                  'iOperatorName']]
#     opdf = t.tables['operator'].get_df()
#     locat = pd.merge(locat,opdf,on='iOperatorName',how='left')
#     df = pd.merge(df,locat,on='UploadKey',how='left')
#     gb = df.groupby(['UploadKey','StateName'],as_index=False)[['CountyName',
#                                                                'OperatorName',
#                                                       'TotalBaseWaterVolume']].first()
#     tmp1 = gb.groupby(['StateName','CountyName','OperatorName'],as_index=False)['UploadKey'].count()
#     tmp1.rename({'UploadKey':'Disclosure_cnt'},inplace=True,axis=1)
#     tmp2 = gb.groupby(['StateName','CountyName','OperatorName'],
#               as_index=False)['TotalBaseWaterVolume'].mean()
#     tmp2.rename({'TotalBaseWaterVolume':'mean_Water_Used'},axis=1,inplace=True)
#     tmp2.mean_Water_Used = tmp2.mean_Water_Used.map(lambda x: round_sig(x,3))
#     out = pd.merge(tmp1,tmp2,on=['StateName','CountyName','OperatorName'],how='left')
#     print(out)
#     out.to_csv(webworkfolder+'daily_data.csv',index=False)
#     #out.to_csv('c:/Users/Gary/Google Drive/webshare/summary_of_today.csv',
#     #   index=False)
# 
# =============================================================================

