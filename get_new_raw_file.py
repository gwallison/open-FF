
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 24 14:43:54 2019

@author: Gary
This script is used to download a new raw set, save it if the 
day of the week is in the list, and look for new events.
It will also record how many records are in each new event, runs
tripwire and uploads a webpage summary for general access.

This script runs independently of the main build_database set.  It is designed 
to run autonomously, can be executed from a crontab command.

occasionally the download won't work, probably because of something on the
FF end.  Currently this script just exits with an exception and does not
retry.
"""

import core.Construct_set as const_set
import core.trip_wire as twire
import pandas as pd
import requests
import subprocess
import shutil
from datetime import datetime
#import hashlib


force_archive = False # use sparingly, only when not doing routine checks.
do_download = True # if False, will run routines without downloading first.
do_tripwire = True
upload_report = True # replaces last report on the web with the todays


today = datetime.today()
if today.weekday() in [4]: # Monday= 0, Sunday = 6
    archive_file=True
else:
    archive_file=False
if force_archive:
    archive_file=True

# define    
sources = './sources/'
archive = './archive/'
datefn= './sources/upload_dates.csv'
skyfn = 'sky_truth_final'
afile = archive+f'ff_archive_{today.strftime("%Y-%m-%d")}.zip'
currfn = 'testData'
lastfn = 'testData_last'
outdir = './out/'
tempfolder = './tmp/'

st = datetime.now() # start timer

# get and save files
if do_download:
    url = 'http://fracfocusdata.org/digitaldownload/fracfocuscsv.zip'
    print(f'Downloading data from {url}')
    r = requests.get(url, allow_redirects=True,timeout=20.0)
    #print(f'Download completed in {endit-st}')
    if do_tripwire:
        twire.backup_testData(infn=currfn+'.zip',
                              outfn=lastfn+'.zip',
                              sources=sources)

    open(sources+currfn+'.zip', 'wb').write(r.content)  # overwrites currfn file.
    if archive_file: open(afile, 'wb').write(r.content)

## Now process file
print('Working on data set')
outdf = pd.read_csv(datefn)
uklst = outdf.UploadKey.unique()
t = const_set.Construct_set(fromScratch=True,
                            zfilename=currfn,
                            sources=sources,
                            outdir=outdir,
                            tempfolder=tempfolder,
                            stfilename=skyfn).get_quick_set()
df = t.get_df_cas(keepcodes='',removecodes='')
ndf = df[~df.UploadKey.isin(uklst)].copy() # just the new ones
gb = ndf.groupby('UploadKey',as_index=False)['iUploadKey'].count()
gb['date_added'] = today.strftime("%Y-%m-%d")
gb.rename({'iUploadKey':'num_records'}, inplace=True,axis=1)

outdf = pd.concat([outdf,gb],sort=True)
outdf.to_csv(datefn,index=False)
t.pickleAll() # pickle so notebook has access to data.


if do_tripwire:
    twire.runTripWire(currfn+'.zip',lastfn+'.zip')

print(f'\nNumber of added events: {len(gb)}\n\n')

if upload_report:
    s= 'jupyter nbconvert --template=nbextensions --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.timeout=-1 --execute daily_report.ipynb --to=html '
    print(subprocess.run(s))
    shutil.copyfile('daily_report.html',
                    'c:/Users/Gary/Google Drive/webshare/daily_report.html')

endit = datetime.now()
print(f'Whole process completed in {endit-st}')

