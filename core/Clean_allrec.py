# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 13:32:22 2019

@author: GAllison

Some things to do initially to the allrec table:
     - add the bgSupplier column (in the supplier table)

     - flag empty events 
The earliest FracFocus data on the pdf website is not included in 
the bulk download.  There are placeholder records in the data set that include
'header' data such as location, operator name, and even the amount of water
used.  However, there are no records about the chemical ingredients in 
the frack.  
Because no chemical disclosure is given for these events, we flag these data
for removal 
from the data set we use for analysis. Keeping them in the data set would
distort any estimates of 'presence/absence' of materials.
Luckily, the organization SkyTruth scraped a lot of that data from the pdf files
and makes it available at their website.  We incorporate it here.

    - flag events with multiple disclosures (done in Clean_event.py)
The FracFocus data set contains multiple versions of some fracking events.
Here we first find the duplicates (using the API number and the fracking date).
NOTE:  For this version of the FF database, we mark ALL duplicates for removal.
While it may be possible to salvage some of the duplicates, there are no direct 
indicators of the most 'correct' version and, according to Mark Layne,
sometimes duplicates are not even replacements, but rather additions to 
previous entries.  (It may be possible to use the position of data entry
to indicate the most recent and presumably correct disclosure entry.)
In this version, we are also not flagging those 'empty events' that are
already flagged.
"""

import pandas as pd
import numpy as np
import core.Column_tools as col_tools


class Clean_allrec():
    def __init__(self,tab_manager = None,sources='./sources/'):  
        self.tab_man = tab_manager
        self.sources = sources
    
    def process_records(self):
        # process Supplier
        df = self.tab_man.tables['supplier'].get_df() # fetch all
        df = col_tools.add_bg_col(df=df,field='Supplier',
                                       sources=self.sources)
        self.tab_man.update_table_df(df,'supplier')
        
        df = self.tab_man.tables['allrec'].get_df() # fetch all
        
        # if there is no IngredientKey, there are no ingredients. Flag for later removal
        print('flagging empty events')
        df.record_flags = np.where(df.ingKeyPresent==False,
                                   df.record_flags.str[:]+'-1',
                                   df.record_flags)
        print(f'  -- flagging {df.record_flags.str.contains("1").sum()} records')
        
        # flagging fracking events that have multiple disclosures
        print('flagging events with multiple disclosures')
        t = self.tab_man.tables['event'].get_df(['UploadKey','iUploadKey',
                                                 'date','APINumber'])
        t = pd.merge(t,df[['iUploadKey','reckey']][~df.record_flags.str.contains('1')],
                     on='iUploadKey',how='right',validate='1:m')
        t = t.groupby(['UploadKey','date','APINumber'],as_index=False)['reckey','iUploadKey'].first()
        t['dupes'] = t.duplicated(subset=['APINumber','date'],keep=False)
        dupes = list(t[t.dupes].iUploadKey.unique())
        print(f'  -- flagging {len(dupes)} disclosures')
        df.record_flags = np.where(df.iUploadKey.isin(dupes),
                                     df.record_flags.str[:]+'-2',
                                     df.record_flags)
        print('flagging events with problems detected in location data')        
        t = self.tab_man.tables['event'].get_df(['UploadKey','iUploadKey',
                                                 'loc_flags'])

        df = pd.merge(df,t[['iUploadKey','loc_flags']],
                           on='iUploadKey',how='left',validate='m:1')
        df.record_flags = np.where(df.loc_flags.isna(),
                                   df.record_flags,
                                   df.record_flags.str[:]+df.loc_flags.str[:])
        df = df.drop('loc_flags',axis=1)
        
        self.tab_man.update_table_df(df,'allrec')
        
        
    
