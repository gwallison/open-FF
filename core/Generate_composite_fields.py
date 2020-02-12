# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 16:52:03 2020

@author: Gary
"""

import pandas as pd
import numpy as np

class Gen_composite_fields():
    def __init__(self,tab_manager = None):  
        self.tab_man = tab_manager
    
    def make_infServiceCo(self):
        """makes the inferred Service Company field.  passed df is an
        allrec table and needs bgSupplier and UploadKey.  Will return
        a table with UploadKey and 'infServiceCo' to be merged into an
        event table"""
        notServ = ['_empty_','_empty_entry_','unrecorded supplier',
                   'Listed Above','not a company','not assigned',
                   'operator','company supplied','ambiguous',
                   '3rd party','multiple suppliers','third party']

        df = self.tab_man.tables['allrec'].get_df() # fetch all
        sup = self.tab_man.tables['supplier'].get_df()[['iSupplier','bgSupplier']]
        df = pd.merge(df,sup,on='iSupplier',how='left')
        #print(df.columns)
        df = df[~df.bgSupplier.isin(notServ)]  # drop all the non-company values
        gb = df.groupby('iUploadKey')['bgSupplier'].agg(lambda x: x.value_counts().index[0])
        gb = gb.reset_index()
        gb.rename({'bgSupplier':'infServiceCo'},axis=1,inplace=True)
        #print(gb.columns)
        ev = self.tab_man.tables['event'].get_df()
        ev = pd.merge(ev,gb,on='iUploadKey',how='left')
        #print(ev.columns)
        self.tab_man.update_table_df(ev,'event')
       
        

