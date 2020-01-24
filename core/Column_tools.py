# -*- coding: utf-8 -*-
"""
Created on Sun Jul 21 16:46:40 2019

@author: Gary
These tools are used to add 'best guess' columns to tables from the xlate files.

"""

import pandas as pd

fields = {'Supplier':'company',
          'OperatorName':'company',
          'StateName':'StateName'}

def add_bg_col(df,field='StateName',sources='./sources/'):
    print(f'Adding column {"bg"+field}')
    fn = sources+fields[field]+'_'+'xlate.csv'
    ref = pd.read_csv(fn,keep_default_na=False,na_values='',quotechar='$',
                      usecols=['primary','original'])
    df['original'] = df[field].str.strip().str.lower()

    ref = ref.rename(columns={'primary':'bg'+field})
    ref = ref[~ref.duplicated(subset='original',keep='last')]
    df = df.merge(ref,on='original',how='left',validate='m:1')
    df = df.drop('original',axis=1)
    return df

