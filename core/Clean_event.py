# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 13:32:22 2019

@author: GAllison

"""

import pandas as pd
import numpy as np
import datetime
import core.Column_tools as col_tools


class Clean_event():
    def __init__(self,tab_manager = None,sources='./sources/'):  
        self.tab_man = tab_manager
        self.sources = sources

# =============================================================================
#     def _adjust_API(self,row):
#         """  Here we create a 10-character string version of the APINumber,
#         which is an integer in the raw data and is susceptible to ambiguity
#         when it comes to identifiying specific wells.  For example,
#         state numbers < 10 are sometimes a problem; The first two digits
#         of the API number should be the state number but they are sometimes
#         wrong because the leading zero is dropped. Colorado must start '05'
#         """
#         s = str(row.APINumber)
#         if int(s[:2])==row.StateNumber:
#             if int(s[2:5])==row.CountyNumber:
#                 return s[:10]  # this is the normal condition, no adjustment needed
#         #print('Adjust_API problem...')
#         s = '0'+s
#         if int(s[:2])==row.StateNumber:
#             if int(s[2:5])==row.CountyNumber:
#                 #self.fflog.add_to_well_log(s[:10],1)
#                 return s[:10]  # This should be the match; 
#         else:
#             # didn't work!
#             s = str(row.APINumber)
#             #self.fflog.add_to_well_log(s[:10],2)
#             print(f'API10 adjustment failed: {row.APINumber}')
#             return s[:10]
#  
#     def _createAPI10(self):
#         print('Creating api10')
#         self.df['api10'] = self.df.apply(lambda x: self._adjust_API(x),axis=1)
#         #print(f'Clean event 1: {len(self.df)}')
# 
# 
# =============================================================================
    def add_upload_bg_cols(self):
        print('Adding columns: bgStateName, bgCountyName, bgLatitude, bgLongitude and date_added')
        ref = pd.read_csv(self.sources+'uploadKey_ref.csv')
        #print(f'Clean event 2: {len(self.df)}')
        self.df = pd.merge(self.df,ref[['UploadKey','bgStateName','bgCountyName',
                                        'bgLatitude','bgLongitude']],
                           on='UploadKey',how='left',validate='1:1')
        #print(f'Clean event 3: {len(self.df)}')
        dates = pd.read_csv(self.sources+'upload_dates.csv')
        #print(f'dates uplkey {dates[dates.UploadKey.duplicated(keep=False)].UploadKey}')
        self.df = pd.merge(self.df,dates[['UploadKey','date_added']],
                           on='UploadKey',how='left',validate='1:1')
        #print(f'Clean event 4: {len(self.df)}')
    
    def make_date_field(self):
        print('Constructing dates')
        # drop the time portion of the datatime
        self.df['d1'] = self.df.JobEndDate.str.split().str[0]
        # fix some obvious typos that cause hidden problems
        self.df['d2'] = self.df.d1.str.replace('3012','2012')
        # instead of translating ALL records, just do uniques records ...
        tab = pd.DataFrame({'d2':list(self.df.d2.unique())})
        tab['date'] = pd.to_datetime(tab.d2)
        # ... then merge back to whole data set
        self.df = pd.merge(self.df,tab,on='d2',how='left',validate='m:1')
        self.df = self.df.drop(['d1','d2'],axis=1)
        
        #convert date_added field
        self.df.date_added = pd.to_datetime(self.df.date_added)
        self.df['pub_delay_days'] = (self.df.date_added-self.df.date).dt.days
        # Any date_added earlier than 10/2018 is unknown
        refdate = datetime.datetime(2018,10,1) # date that I started keeping track
        self.df.pub_delay_days = np.where(self.df.date_added<refdate,
                                          np.NaN,
                                          self.df.pub_delay_days)# is less recent than refdate
        # any fracking date earlier than 4/1/2011 is before FF started taking data
        refdate = datetime.datetime(2011,4,1) # date that fracfocus started
        self.df.pub_delay_days = np.where(self.df.date<refdate,
                                          np.NaN,
                                          self.df.pub_delay_days)# is less recent than refdate
        
    
    def process_events(self):
        self.df = self.tab_man.tables['event'].get_df() # fetch all  
        #print(f'Clean event: {len(self.df)}')
        #self._createAPI10()
        self.add_upload_bg_cols()
        #print(f'Clean event: {len(self.df)}')

        self.make_date_field()
        #print(f'Clean event: {len(self.df)}')
        self.tab_man.update_table_df(self.df,'event')
        #print(f'Clean event: {len(self.df)}')

        # now process OperatorName
        self.df = self.tab_man.tables['operator'].get_df() # fetch all
        self.df = col_tools.add_bg_col(df=self.df,field='OperatorName',
                                       sources=self.sources)
        self.tab_man.update_table_df(self.df,'operator')
        
    
