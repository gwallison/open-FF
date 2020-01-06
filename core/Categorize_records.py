# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 07:59:02 2019

@author: Gary Allison
"""
import pandas as pd
import numpy as np
import core.CAS_tools as ct
#import core.process_CAS_ref_files

class Categorize_CAS():
    """ This class sorts all data in the dataframe into one of the following
    categories:
        - valid data record (has valid CAS id)
        - proprietary data
        - non-data 
        - hidden data (has quantity but identity is masked in some way)
        
    This work proceeds in phases:
    Phase I:    Find records which match CAS authority. 
    Phase II:   Mark the records that are explicitly labeled with some form
                of 'proprietary' (explicitly hidden) and other hidden labels
                (implicitly hidden)
    Phase III:  Mark records that are duplicates *within* a given event.  These
                records are apparently generated during the process of translating
                from the pdf files to the data files, and should be ignored. 
                There are over 70,000 of them.
    
        """
        
    def __init__(self,tab_manager=None,sources='./sources/',
                 outdir='./out/'):
        """
        """
        self.tab_man = tab_manager
        self.df = self.tab_man.get_df_cas(keepcodes='',removecodes='')
        #print(f'init cat_rec df len = {len(self.df)}, reckey len = {len(self.df.reckey.unique())}')
        self.cas_ref_dict = self.get_cas_ref_dict(sources+'CAS_ref_and_names.csv')
        
        self._get_cas_field_cat()
        # the cas_labels file has a list of all the CASNumbers with a
        # column that identifies the labels that signify the 'proprietary' status
        # Those identifiers were added manually through inspection of the CAS list.
        self.cas_labels_fn = sources+'cas_labels.csv'
        self.replace_cas_fn = sources+'cas_to_rename.csv'
        self.replace_dict = self._get_replacement_cas() 
        self.v_b_e_fn = sources+'valid_but_empty.csv'
        self.valid_but_empty = self._get_valid_but_empty() #list
        #self.proprietary_list = sources+'CAS_labels_for_proprietary.csv'
        #self.refdir = './CAS_ref/out/'
    
    def get_cas_ref_dict(self,fn):
        df = pd.read_csv(fn,quotechar='$')
        dic = {}
        #casl = list(df.cas_number.keys())
        for i in df.index:
            dic[df.at[i,'cas_number']] = df.at[i,'ing_name'] 
        return dic
        

    def _get_cas_field_cat(self):
        self.cas_field_cat = self.df.groupby('CASNumber',as_index=False)['UploadKey'].count()
        self.cas_field_cat.rename({'UploadKey':'total_cnt'},inplace=True,axis=1)
        print(f'Number of unique raw CASNumbers: {len(self.cas_field_cat)}')

    def _get_replacement_cas(self):
        """There are a handful of CAS numbers used in FF that are valid but
        are considered by SciFinder and other sources to be obsolete.  We use
        this table to translate the obsolete name to the cas# more widely used.
        """
        tmp = {}
        self.replace_cas = pd.read_csv(self.replace_cas_fn)
        ori = list(self.replace_cas.original)
        ren = list(self.replace_cas.replacement)
        for cnt,o in enumerate(ori):
            tmp[o] = ren[cnt]
        print(f'Obsolete but replacable cas numbers registered: {len(tmp.keys())}')
        return tmp

    def _get_valid_but_empty(self):
        t = pd.read_csv(self.v_b_e_fn)
        print(f'Valid_but_empty cas numbers registered: {len(t)}')
        return list(t.valid_but_empty)
    
###  Phase I - find valid records based on legimate CASNumber
        
    def _replace_obsolete(self,cas):
        if cas in self.replace_dict.keys():
            return self.replace_dict[cas]
        return cas
        
    def _clean_CAS_for_comparison(self):
        #print('clean cas for comparison')
        self.cas_field_cat['cas_clean'] = self.cas_field_cat.CASNumber.str.replace(r'[^0-9-]','')
        self.cas_field_cat['zero_corrected'] = self.cas_field_cat.cas_clean.map(lambda x: ct.correct_zeros(x) )
        # replace the handful of obsolete cas numbers with widely used number
        self.cas_field_cat.zero_corrected = self.cas_field_cat.zero_corrected.map(lambda x: self._replace_obsolete(x))
        
    def _getIgName(self,cas):
        if cas in self.cas_ref_dict:
            return self.cas_ref_dict[cas]
        else:
            return 'name_unresolved'
        
    def _mark_if_perfect_match(self):
        self.cas_ref_lst = list(self.cas_ref_dict.keys())
        self.cas_field_cat['perfect_match'] = self.cas_field_cat.zero_corrected.isin(self.cas_ref_lst)
        self.cas_field_cat['bgCAS'] = np.where(self.cas_field_cat.perfect_match,
                                               self.cas_field_cat.zero_corrected,
                                               'cas_unresolved')
        self.cas_field_cat['bgIngredientName'] = self.cas_field_cat.bgCAS.map(lambda x: self._getIgName(x))
        tmp = self.cas_field_cat[['CASNumber','perfect_match','bgCAS','bgIngredientName']].copy()
        self.df = pd.merge(self.df,tmp,on='CASNumber',how='left',validate='m:1')
        
    def phaseI(self):
        self._clean_CAS_for_comparison()
        self._mark_if_perfect_match()
        print(f'Number of perfect matches from unique CAS: {self.cas_field_cat.perfect_match.sum()}')
        print(f'Total records affected:    {self.df.perfect_match.sum()}\n')
        self.df.record_flags = np.where(self.df.perfect_match,
                                   self.df.record_flags.str[:]+'-P',
                                   self.df.record_flags)
        
    def get_corrected_not_perf(self):
        """Used when updating all the curation files when new records are added
        to database"""
        self._clean_CAS_for_comparison()
        self._mark_if_perfect_match()
        c1 = self.cas_field_cat.perfect_match==False
        c2 = ~self.cas_field_cat.zero_corrected.isin(self.valid_but_empty)
        c3 = ~self.cas_field_cat.zero_corrected.isin(self.replace_dict.keys())
        return self.cas_field_cat[c1&c2&c3].zero_corrected
    
###  Phase II - Proprietary claims and other hidden labels
    def _add_proprietary_column(self):
        labels = pd.read_csv(self.cas_labels_fn,keep_default_na=False,na_values='')
        prop_lst = list(labels[labels.proprietary==1].clean.str.lower().str.strip().unique())
        self.cas_field_cat['proprietary'] = self.cas_field_cat.CASNumber.str.lower().str.strip().isin(prop_lst)
        tmp = self.cas_field_cat[['CASNumber','proprietary']].copy()
        self.df = pd.merge(self.df,tmp,on='CASNumber',how='left',validate='m:1')
        
    def _add_hiding_column(self):
        labels = pd.read_csv(self.cas_labels_fn,keep_default_na=False,na_values='')
        c1 = (labels.multiple==1)|(labels.non_cas==1)
        hiding_lst = list(labels[c1].clean.str.lower().str.strip().unique())
        self.cas_field_cat['un_cas_like'] = self.cas_field_cat.CASNumber.str.lower().str.strip().isin(hiding_lst)
        tmp = self.cas_field_cat[['CASNumber','un_cas_like']].copy()
        self.df = pd.merge(self.df,tmp,on='CASNumber',how='left',validate='m:1')

    def phaseII(self):
        """record_flags for explicit proprietary is 3
                    for non_cas_like CAS Number but with quantity = 4
                    for non_cas_like CAS Number but absent quantity = 5"""
        self._add_proprietary_column()
        print(f'Total Proprietary records= {self.df.proprietary.sum()}')
        
        self.df.record_flags = np.where(self.df.proprietary,
                                   self.df.record_flags.str[:]+'-3',
                                   self.df.record_flags)
        
        
        self._add_hiding_column()
        cond1 = self.df.PercentHFJob>0
        cond2 = self.df.MassIngredient>0
        has_quant = cond1 | cond2
        not_quant = ~has_quant
        cond3 = self.df.un_cas_like
        self.df.record_flags = np.where(cond3&has_quant,
                                self.df.record_flags.str[:]+'-4',
                                self.df.record_flags)
        self.df.record_flags = np.where(cond3&not_quant,
                                self.df.record_flags.str[:]+'-5',
                                self.df.record_flags)
        print(f'Total Non_caslike but quant = {len(self.df[self.df.record_flags.str.contains("4",regex=False)])}')
#        print(f'Total Non_caslike but not quant = {len(t[t.record_flags==5])}')
        
### Phase III - check for ingredient duplicates within events
    def _flag_duplicated_records(self):
        self.df['dup'] = self.df.duplicated(subset=['UploadKey','IngredientName',
                                       'CASNumber','MassIngredient','PercentHFJob'],
                                        keep=False)
        #print(f'df len = {len(self.df)}, reckey len = {len(self.df.reckey.unique())}')
        c0 = self.df.ingKeyPresent
        cP = self.df.record_flags.str.contains('P',regex=False)
        dups = self.df[(self.df.dup)&(c0)&(cP)].copy()
        c1 = dups.Supplier.str.lower().isin(['listed above'])
        c2 = dups.Purpose.str.lower().str[:9]=='see trade'
        dups['redundant_rec'] = c1&c2
        
# =============================================================================
#         dups['dup_reckey'] = dups.reckey.duplicated(keep=False)
#         upk = list(dups[dups.dup_reckey].UploadKey.unique())
#         print(f'Dup repkeys at: {upk}')
#         print(f' Expected redundant total: {dups.redundant_rec.sum()}, {c1.sum()}, {c2.sum()}')
#         print(f'dups len = {len(dups)}, reckey len = {len(dups.reckey.unique())}')
# =============================================================================
        
        self.df = pd.merge(self.df,dups[['reckey','redundant_rec']],
                           on='reckey',how='left',validate='m:1')

    def phaseIII(self):
        """ > 70000 records are duplicated within events apparently due to the
        process of converting the pdf files to the bulk download.  These duplicates
        are identifiable by their 'Supplier' and 'Purpose' fields. Here we identify
        all duplicates (by 5 fields) then flag those that have the supplier/purpose
        characteristic -- record_flags is R."""
        self._flag_duplicated_records()
        self.df.record_flags = np.where(self.df.redundant_rec==True,
                                self.df.record_flags.str[:]+'-R',
                                self.df.record_flags)
        print(f'Total redundant records flagged: {len(self.df[self.df.record_flags.str.contains("R",regex=False)])}')
        
    def do_all(self):
        #print(f'preI df len = {len(self.df)}, reckey len = {len(self.df.reckey.unique())}')
        #print(f'pre-phase 1:  Len df: {len(self.df)} ')
        self.phaseI()
        #print(f'preII df len = {len(self.df)}, reckey len = {len(self.df.reckey.unique())}')
        #print(f'phase 1:  Len df: {len(self.df)} ')
        self.phaseII()
        #print(f'pre III df len = {len(self.df)}, reckey len = {len(self.df.reckey.unique())}')
        #print(f'phase 2:  Len df: {len(self.df)} ')
        self.phaseIII()
        #print(f'postIII df len = {len(self.df)}, reckey len = {len(self.df.reckey.unique())}')
        # saves new flags
        self.tab_man.tables['allrec'].replace_df(self.df)
        gb = self.df.groupby('iCASNumber',as_index=False)['bgCAS',
                                                        'bgIngredientName',
                                                        'proprietary'].first()
        self.tab_man.tables['cas'].merge_df(gb)
        return self.df