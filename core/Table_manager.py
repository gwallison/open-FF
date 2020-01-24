# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 14:19:41 2019

@author: Gary
"""
import pandas as pd
#import pickle
import core.FF_table as fft

class Construct_tables():
    
    def __init__(self,pkldir='./tmp/'):
        self.cas = fft.FF_table(keyf='CASNumber',other_fields=['iCASNumber'],pkldir=pkldir)
        self.supplier = fft.FF_table(keyf='Supplier',other_fields=['iSupplier'],pkldir=pkldir)
        self.purpose = fft.FF_table(keyf='Purpose',other_fields=['iPurpose'],pkldir=pkldir)
        self.tradename = fft.FF_table(keyf='TradeName',other_fields=['iTradeName'],pkldir=pkldir)
        self.operator = fft.FF_table(keyf='OperatorName',other_fields=['iOperatorName'],pkldir=pkldir)
        self.ingName = fft.FF_table(keyf='IngredientName',other_fields=['iIngredientName'],pkldir=pkldir)
        self.event = fft.FF_table(keyf='UploadKey',pkldir=pkldir,
                                  other_fields=['JobEndDate','JobStartDate',
                                                'iOperatorName',
                                                'APINumber',
                                                'TotalBaseWaterVolume',
                                                'TotalBaseNonWaterVolume',
                                                'FFVersion','TVD',
                                                'StateName','StateNumber',
                                                'CountyName','CountyNumber',
                                                'Latitude','Longitude',
                                                'WellName','FederalWell',
                                                'IndianWell','data_source',
                                                'iUploadKey'])
        self.allrec = fft.FF_table(keyf='reckey',pkldir=pkldir,
                                   other_fields=['iSupplier',
                                                 'iPurpose','iTradeName',
                                                 'PercentHFJob','MassIngredient',
                                                 'ingKeyPresent',
                                                 'iUploadKey', #'iAPINumber',
                                                 'iCASNumber','iIngredientName',
                                                 'record_flags','ireckey'
                                                 ])
        self.tables = {'cas' : self.cas,    
                       'supplier' : self.supplier,
                       'operator' :self.operator,
                       'purpose' : self.purpose,
                       'tradename': self.tradename,
                       'ingName' : self.ingName,
                       'event' : self.event,
                       'allrec' : self.allrec,
                       }

    def add_indexes_to_full(self,raw_df):
        print('Adding indicies to raw df')
        for tn in self.tables:
            name = self.tables[tn].keyf
            print(f' -- {"i"+name}')
            if raw_df[name].dtype=='int64': raw_df[name].fillna(-1,inplace=True)
            else: raw_df[name].fillna('_empty_',inplace=True)
            l = list(raw_df[name].unique())
            df = pd.DataFrame({name:l})
            df['i'+name] = df.index.astype(int)
            raw_df = raw_df.merge(df,on=name,how='left')
        return raw_df

    def build_tables(self,raw_df):
        print('Building tables')
        for tn in self.tables:
            name = self.tables[tn].keyf
            print(f' -- {tn} has {name}...')
            self.tables[tn].construct_df(raw_df)

    def addToTables(self,df):
        for tn in self.tables:
            self.tables[tn]._construct_from_df(df)
            
    def update_table_df(self,df,tn):
        self.tables[tn].df = df.copy()
        self.tables[tn].update()
            
    def pickleAll(self,tmp='./tmp/'):
        for tn in self.tables:
            self.tables[tn].pickleComponents()
            
    def loadAllPickles(self):
        for tn in self.tables:
            self.tables[tn].unPickleComponents()

    def listTables(self,show_all=False):
        for table in self.tables:
            print(f'Table "{table}" keyed with {self.tables[table].keyf} has {self.tables[table].num_entries()} records')
            if show_all: self.tables[table].show_diag()

### ----  fetch some standard data frames
            
    def get_df_cas(self,cas_fields=[],ing_fields=[],
                   op_fields=[],sup_fields=[],pur_fields=[],
                   event_fields=['UploadKey','iUploadKey','iOperatorName',
                                 'APINumber','TotalBaseWaterVolume'],
                                 # force explicit use of keep/remove codes
                                 keepcodes = 'empty', #'M|3|A',
                                 removecodes = 'empty', #'R|1|2|4|5'
                                 ):
        """if cas_fields and ing_fields left empty, all fields will be merged
        from those tables (also sup and pur fields)
        keepcodes and removecodes must be supplied explicitly"""
        df = self.tables['allrec'].get_df()
        print(f'   Initial length before filter:  {len(df.ireckey)}')
        assert keepcodes!='empty', 'Must specify keepcodes!'
        assert removecodes!='empty', 'Must specify removecodes!'
        if keepcodes: df = df[df.record_flags.str.contains(keepcodes)]
        if removecodes: df = df[~df.record_flags.str.contains(removecodes)]
        print(f'                  after  filter:  {len(df.ireckey)}')
        
        
        df = df.merge(self.tables['event'].get_df(event_fields),
                      on='iUploadKey',how='left',validate='m:1')
        df = df.merge(self.tables['ingName'].get_df(ing_fields),
                      on='iIngredientName',
                      how='left',validate='m:1')
        df = df.merge(self.tables['supplier'].get_df(sup_fields),
                      on='iSupplier',
                      how='left',validate='m:1')
        df = df.merge(self.tables['operator'].get_df(op_fields),
                      on='iOperatorName',
                      how='left',validate='m:1')
        df = df.merge(self.tables['purpose'].get_df(pur_fields),
                      on='iPurpose',how='left',validate='m:1')
        return df.merge(self.tables['cas'].get_df(cas_fields),
                        on='iCASNumber',how='left',validate='m:1')

    def get_df_location(self,event_fields=[]):
        """if loc_fields and ing_fields left empty, all fields will be merged
        from those tables"""
        return self.tables['event'].get_df(fields=event_fields)
