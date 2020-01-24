
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
from datetime import datetime

today = datetime.today()
if today.weekday() in [0]: # Monday= 0, Sunday = 6
    archive_file=True
else:
    archive_file=False

# define    
sources = './sources/'
archive = './archive/'
datefn= './out/upload_dates.csv'
skyfn = sources+'sky_truth_final.zip'
afile = archive+f'ff_archive_{today.strftime("%Y-%m-%d")}.zip'
currfn = sources+'testData.zip'
outdir = './out/'
tempfolder = './tmp/'

# get and save files
url = 'http://fracfocusdata.org/digitaldownload/fracfocuscsv.zip'
print(f'Downloading data from {url}')
r = requests.get(url, allow_redirects=True)
print('Download completed')
open(currfn, 'wb').write(r.content)  # overwrites last file.
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
df = df[~df.UploadKey.isin(uklst)]
gb = df.groupby('UploadKey',as_index=False)['iUploadKey'].count()
gb['date_added'] = today.strftime("%Y-%m-%d")
#gb['date_added'] = "2019-12-21" ##today.strftime("%Y-%m-%d")
gb.rename({'iUploadKey':'num_records'}, inplace=True,axis=1)
print(f'New events: {len(gb)}')
#print(gb.head())
outdf = pd.concat([outdf,gb],sort=True)
outdf.to_csv(datefn,index=False)
