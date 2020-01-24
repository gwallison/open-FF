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
    print('Adding Elsner/Hoelzer table to CAS table')
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

    tab_manager.tables['cas'].replace_df(mg,add_all=True)

    
def add_TEDX_ref(tab_manager=None,sources='./sources/',
                 outdir='./out/',
                 tedx_fn = 'TEDX_EDC_trimmed.xls'):
    """Add TEDX link"""
    print('Adding TEDX link to CAS table')
    casdf = tab_manager.tables['cas'].get_df()
    tedxdf = pd.read_excel(sources+tedx_fn)
    tedx_cas = tedxdf.CAS_Num.unique().tolist()
    casdf['is_on_TEDX'] = casdf.bgCAS.isin(tedx_cas)
    tab_manager.tables['cas'].replace_df(casdf,add_all=True)
    
def add_TSCA_ref(tab_manager=None,sources='./sources/',
                 outdir='./out/',
                 tsca_fn = 'TSCAINV_092019.csv'):
    print('Adding TSCA to CAS table')
    casdf = tab_manager.tables['cas'].get_df()
    tscadf = pd.read_csv(sources+tsca_fn)
    tsca_cas = tscadf.CASRN.unique().tolist()
    casdf['is_on_TSCA'] = casdf.bgCAS.isin(tsca_cas)
    tab_manager.tables['cas'].replace_df(casdf,add_all=True)
    