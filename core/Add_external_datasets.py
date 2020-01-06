# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 09:32:29 2019

@author: Gary
"""
import pandas as pd

def add_Elsner_table(tab_manager=None,sources='./sources/',
                     outdir='./out/',
                     ehfn='elsner_corrected_table.csv'):
    """Add the Elsner/Hoelzer data table. """
    print('Adding Elsner/Hoelzer table to CAS')
    casdf = tab_manager.tables['cas'].get_df()
    ehdf = pd.read_csv(sources+ehfn,quotechar='$')
    # checking overlap first:
    ehcas = list(ehdf.eh_CAS.unique())
    dfcas = list(casdf.bgCAS.unique())
    with open(outdir+'elsner_non_overlap.txt','w') as f:
        f.write('**** bgCAS numbers without an Elsner entry: *****\n')
        for c in dfcas:
            if c not in ehcas:
                f.write(f'{c}\n')
        f.write('\n\n***** Elsner CAS numbers without a FF entry: *****\n')
        for c in ehcas:
            if c not in dfcas:
                f.write(f'{c}\n')

    mg = pd.merge(casdf,ehdf,left_on='bgCAS',right_on='eh_CAS',
                  how='left',validate='m:1')
    #print(mg.columns)
    tab_manager.tables['cas'].replace_df(mg,add_all=True)
    #print(tab_manager.tables['cas'].df.columns)
    