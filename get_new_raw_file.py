
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

force_archive = False # use sparingly, only when not doing routine checks.
do_download = True # if False, will run routines without downloading first.



today = datetime.today()
if today.weekday() in [0]: # Monday= 0, Sunday = 6
    archive_file=True
else:
    archive_file=False
if force_archive:
    archive_file=True

# define    
sources = './sources/'
archive = './archive/'
datefn= './out/upload_dates.csv'
skyfn = 'sky_truth_final'
afile = archive+f'ff_archive_{today.strftime("%Y-%m-%d")}.zip'
currfn = 'testData'
outdir = './out/'
tempfolder = './tmp/'

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
print(f'Number of added events: {len(gb)}\n\n')

outdf = pd.concat([outdf,gb],sort=True)
outdf.to_csv(datefn,index=False)
t.pickleAll() # pickle so notebook has access to data.

s= 'jupyter nbconvert --template=nbextensions --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.timeout=-1 --execute daily_report.ipynb --to=html '
print(subprocess.run(s))
shutil.copyfile('daily_report.html',
                'c:/Users/Gary/Google Drive/webshare/daily_report.html')


