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

    def add_upload_bg_cols(self):
        print('Adding columns: bgStateName, bgCountyName, bgLatitude, bgLongitude and date_added')
        ref = pd.read_csv(self.sources+'uploadKey_ref.csv')

        self.df = pd.merge(self.df,ref[['UploadKey','bgStateName','bgCountyName',
                                        'bgLatitude','bgLongitude',
                                        'loc_flags']],
                           on='UploadKey',how='left',validate='1:1')
        
        dates = pd.read_csv(self.sources+'upload_dates.csv')

        self.df = pd.merge(self.df,dates[['UploadKey','date_added']],
                           on='UploadKey',how='left',validate='1:1')



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
        
        # convert JoBStartDate as secondary (sometimes needed for SkyTruth?)
        self.df['d1'] = self.df.JobStartDate.str.split().str[0]
        # fix some obvious typos that cause hidden problems
        self.df['d2'] = self.df.d1.str.replace('3012','2012')
        # instead of translating ALL records, just do uniques records ...
        tab = pd.DataFrame({'d2':list(self.df.d2.unique())})
        tab['alt_date'] = pd.to_datetime(tab.d2)
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

        
    def generate_skytruth_flags(self):
        self.df['api10'] = self.df.APINumber.str[:10]
        # we will need to identify bulk "empty" events, so bring in ingKeyPresent
        arec = self.tab_man.tables['allrec'].get_df(['iUploadKey',
                                                    'ingKeyPresent'])
        # if an event has ANY chemical records, say it is not empty 
        keytab = arec.groupby('iUploadKey')['ingKeyPresent'].any()
        self.df = pd.merge(self.df,keytab,on='iUploadKey',how='left')
        
        # try matches with the standard date (enddate)
        bulk = self.df[self.df.data_source=='bulk'][['iUploadKey','api10',
                                                     'date','ingKeyPresent']]
        bulk.columns = ['iUpCopy','api10','date','ingKeyPresent']
        mg1 = pd.merge(self.df[self.df.data_source=='SkyTruth'][['iUploadKey',
                                                                 'api10','date']],
                       bulk, on=['api10','date'],how='left')

        # if any bulk match has ingKeyPresent, then flag with be 'extra'
        # this next step gets us the acceptable events: date match, with ingKey=False
        bg1 = mg1.groupby('iUploadKey',as_index=False)['ingKeyPresent'].any()
        
        self.df['flag'] = '* skytruth_flag_mistake *' # default should be overwritten
        self.df.flag = np.where(self.df.data_source=='bulk','',self.df.flag)
        self.df.flag = np.where(self.df.data_source=='SkyTruth','-e',self.df.flag)
        plus = bg1[bg1.ingKeyPresent==False].iUploadKey.tolist()
        nomatch = mg1[mg1.iUpCopy.isna()].iUploadKey.tolist()
        self.df.flag = np.where(self.df.iUploadKey.isin(plus),'-+',self.df.flag)        
        self.df.flag = np.where(self.df.iUploadKey.isin(nomatch),'-?',self.df.flag)   
        
        # because APINumber is only valid at 10 digits here, we also do duplicate tests
        stonly = self.df[self.df.data_source=='SkyTruth'][['APINumber','iUploadKey']].copy()
        stonly['api10'] = stonly.APINumber.str[:10]
        st_iUp = stonly[stonly.api10.duplicated(keep=False)].iUploadKey.tolist()
        self.df.flag = np.where(self.df.iUploadKey.isin(st_iUp),
                                self.df.flag.str[:]+'-2',
                                self.df.flag)

        lookup = self.df[['iUploadKey','flag']]

        #lookup.to_csv('./tmp/st_valid.csv')

        arec = self.tab_man.tables['allrec'].get_df()
        
        arec = pd.merge(arec,lookup,on='iUploadKey',how='left',validate='m:1')
        arec.record_flags = arec.record_flags.str[:] + arec.flag
        arec = arec.drop(['flag'],axis=1)

        self.tab_man.update_table_df(arec,'allrec')

        self.df = self.df.drop(['api10','ingKeyPresent','flag'],axis=1)
        
    def pair_skytruth_with_bulk(self):
        """ This set of code is used (not during build time) to create an
        list of skytruth pairings with bulk download.  It shows UploadKeys
        and duplicated disclosures; where there are no pairings, etc."""

        # create lists of APIs in the different groups
        self.df['api10'] = self.df.APINumber.str[:10]
        self.df['is_st'] = self.df.UploadKey.str[:3]=='SKY'

        arec = self.tab_man.tables['allrec'].get_df(['iUploadKey',
                                                    'ingKeyPresent'])
        # if an event has ANY chemical records, say it is not empty
        keytab = arec.groupby('iUploadKey')['ingKeyPresent'].any()
        self.df = pd.merge(self.df,keytab,on='iUploadKey',how='left')
        
        # try matches with the standard date (enddate)
        bulk = self.df[~self.df.is_st][['iUploadKey','api10',
                                        'date','ingKeyPresent','UploadKey']]
        bulk.columns = ['iUpCopy','api10','date','ingKeyPresent','UploadKey_bulk_match']
        bulk['date_useage'] = 'with_end_date'
        tmp1 = pd.merge(self.df[self.df.is_st][['iUploadKey','api10','date','UploadKey']],
                       bulk, on=['api10','date'],how='left')
        
        # for those without matches, try the alternate date
        nomatch = tmp1[tmp1.iUpCopy.isna()].iUploadKey.tolist()
        bulk = self.df[~self.df.is_st][['iUploadKey','api10',
                                        'alt_date','ingKeyPresent','UploadKey']]
        bulk.columns = ['iUpCopy','api10','date','ingKeyPresent','UploadKey_bulk_match']
        bulk['date_useage'] = 'with_alt_date'
        tmp2 = pd.merge(self.df[self.df.iUploadKey.isin(nomatch)][['iUploadKey',
                                                                   'api10','date',
                                                                   'UploadKey']],
                       bulk, on=['api10','date'],how='left')
        tmp3 = pd.concat([tmp1[~tmp1.iUpCopy.isna()],tmp2],sort=True)
        tmp3.to_csv('./tmp/st_test.csv')                     
        
        # for those STILL without matches, try without date
        nomatch = tmp3[tmp3.iUpCopy.isna()].iUploadKey.tolist()
        bulk = self.df[~self.df.is_st][['iUploadKey','api10',
                                        'date','ingKeyPresent','UploadKey']]
        bulk.columns = ['iUpCopy','api10','bulk_date','ingKeyPresent','UploadKey_bulk_match']
        bulk['date_useage'] = 'without_date'
        tmp4 = pd.merge(self.df[self.df.iUploadKey.isin(nomatch)][['iUploadKey',
                                                                   'api10','date',
                                                                   'UploadKey']],

                        bulk, on=['api10'],how='left')

        nomatch = tmp4[tmp4.iUpCopy.isna()].iUploadKey.tolist()
        tmp5 = pd.concat([tmp3[~tmp3.iUpCopy.isna()],tmp4],sort=True)        
        tmp5['st_duped'] = tmp5.iUploadKey.duplicated(keep=False)
        tmp5['bulk_duped'] = tmp5.iUpCopy.duplicated(keep=False)
        tmp6 = tmp5.groupby('iUploadKey',as_index=False)['ingKeyPresent'].any()

        tmp6.to_csv('./tmp/temp.csv')
        
        tmp5['flag'] = '?'
        tmp5.flag = np.where(tmp5.ingKeyPresent==True,'e',tmp5.flag)
        tmp5.flag = np.where((tmp5.ingKeyPresent==False)&(tmp5.date_useage=='with_end_date'),
                             '+',tmp5.flag)
        
        tmp5.to_csv('./out/skytruth_status.csv')                

        print(f'Num duplicates + skytruth = {tmp5[tmp5.flag=="+"].iUploadKey.duplicated(keep=False).sum()}')
        print(f'Unique flags {tmp5.flag.unique()}')
        # now add the flag to the record flags
        arec = self.tab_man.tables['allrec'].get_df()

        for fg in ['+','e','?']:
            st_iUp = tmp5[tmp5.flag==fg].iUploadKey.unique().tolist()
            print(f'  -- adding {fg} flag to {len(st_iUp)} records')
            arec.record_flags = np.where(arec.iUploadKey.isin(st_iUp),
                                         arec.record_flags.str[:] + '-' + fg,
                                         arec.record_flags)

        self.tab_man.update_table_df(arec,'allrec')
        
        
    def process_events(self):
        self.df = self.tab_man.tables['event'].get_df() # fetch all  
        self.add_upload_bg_cols()
        self.make_date_field()
        self.tab_man.update_table_df(self.df,'event')

        print('Generate Skytruth flags')
        self.generate_skytruth_flags()
        self.tab_man.update_table_df(self.df,'event')
      
        # now process OperatorName
        self.df = self.tab_man.tables['operator'].get_df() # fetch all
        self.df = col_tools.add_bg_col(df=self.df,field='OperatorName',
                                       sources=self.sources)
        self.tab_man.update_table_df(self.df,'operator')
        
    
