# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 21:17:13 2019

@author: Gary
"""
import pandas as pd

class FF_table():
    """ for tables with single field as key"""
    
    def __init__(self,keyf='UploadKey',
                 other_fields=[],
                 idx_sources=[],
                 pkldir = './tmp/'):
        self.keyf = keyf
        self.other_fields = other_fields
        self.pkldir = pkldir
        self.pkl_fn = self.pkldir+self.keyf+'_df.pkl'
        #self.keyname = 'p'+keyf
        #self.idx_sources = idx_sources 
        #self.key_to_idx = {}
        #self.idx_to_key = {}
        self.df = pd.DataFrame()
        #self.idx_kernel =  pd.DataFrame()
        
    def construct_df(self,raw_df):
        self.df = raw_df.groupby(self.keyf,as_index=False)[self.other_fields].first()
        #if self.keyf=='UploadKey':
        #    print(f'FF_Table (event) {len(self.df)}')
        
    def pickleComponents(self):
        #print(f'Pickling {self.keyf} with fn_prefix= {fn_prefix}')
        self.df.to_pickle(self.pkl_fn)

    def unPickleComponents(self):
        self.df = pd.read_pickle(self.pkl_fn)
        self.other_fields = list(self.df.columns)

    def get_df(self,fields=[]):
        """ if fields==[], return all fields"""
        if fields: return self.df[fields].copy()
        return self.df.copy()
    
    def merge_df(self,new_df=None):
        """used to put new columns into the data set
        Assumes incoming df has just index and columns to be added"""
        self.df = pd.merge(self.df,new_df,on='i'+self.keyf)
        #print(f'>> merging {self.keyf}; {self.df.columns}')
        self.update()

    def replace_df(self,new_df=None,add_all=False,added_fields=[]):
        """replaces existing df with new, and only keeps the original fields
        plus any in added_fields list"""
        if add_all==False:
            fields = self.other_fields + added_fields
            self.df = new_df[fields]
        else:
            self.df = new_df
        self.update()

    def update(self):
        """after df has changed, use this to update everything"""
        self.other_fields = list(self.df.columns)
        self.pickleComponents()
        
    def num_entries(self):
        return len(self.df)
    
    def show_diag(self):
        for col in list(self.df.columns):
            if self.df[col].dtype in ['int64','object']:
                print(f' Column < {col} > has {len(self.df[col].unique())} unique items')