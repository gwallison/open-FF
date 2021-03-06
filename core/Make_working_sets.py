# -*- coding: utf-8 -*-
"""
Created on Wed Sep 25 12:04:34 2019

@author: Gary

This module is used to generate and store the dataframes that
can be downloaded and used for analysis
"""
import zipfile
#import shutil
import os
import shutil

class Make_working_sets():
    def __init__(self,tab_man,outdir='./out/',tmpdir='./tmp/'):
        self.tab_man = tab_man
        self.outdir = outdir
        self.tmpdir = tmpdir
        self.filtered_fields = ['reckey', 'PercentHFJob', 'record_flags', 
                                'bgMass', 'UploadKey', 
                                'OperatorName','bgOperatorName',
                                'APINumber', 'TotalBaseWaterVolume',
                                'TotalBaseNonWaterVolume', 'FFVersion', 
                                'TVD', 'StateName', 'StateNumber', 'CountyName', 
                                'CountyNumber', 'Latitude', 'Longitude',
                                'Projection','bgLatitude', 'bgLongitude', 
                                'data_source', 'bgStateName', 'bgCountyName', 
                                'date', 'WellName',
                                'IngredientName', 'Supplier', 'bgSupplier', 
                                'Purpose', 'TradeName', 'CASNumber', 'bgCAS',
                                'bgIngredientName', 'proprietary', 
                                'eh_Class_L1', 'eh_Class_L2', 'eh_CAS', 
                                'eh_IngredientName', 'eh_subs_class', 
                                'eh_function','is_on_TEDX',
                                'we_Pathway','we_Toxicity','we_FoodAdditive',
                                'we_T3DB','we_ProteinTargets']
        
    def save_compressed(self,df,fn):
        tmpfn = fn+'.csv'
        df.to_csv(tmpfn,index=False) # write in default directory for CodeOcean
        with zipfile.ZipFile(self.outdir+fn+'.zip','w') as z:
             z.write(tmpfn,compress_type=zipfile.ZIP_DEFLATED)
        os.remove(tmpfn)
        
    def save_compressed_pickle(self,df,fn):
        tmpfn = fn+'.pkl'
        df.to_pickle(tmpfn) # write in default directory for CodeOcean
        with zipfile.ZipFile(self.outdir+fn+'.zip','w') as z:
             z.write(tmpfn,compress_type=zipfile.ZIP_DEFLATED)
        os.remove(tmpfn)
        
    def save_full_set(self):
        print('   ** making "df_full_flat.zip" set **')
        full = self.tab_man.get_df_cas(keepcodes='',removecodes='',
                                       event_fields=[])
        full = full.drop(['iSupplier','iPurpose','iTradeName','iUploadKey',
                          'iCASNumber','iIngredientName','ireckey',
                          'iOperatorName','is_carrier'],axis=1)
        self.save_compressed_pickle(full,'full_pickle')
        self.save_compressed(full,'df_full_flat')
        
    def save_filtered_set(self,col_list=None):
        print('   ** making "df_filtered_flat.zip and filtered pickle" set **')
        if col_list==None : col_list = self.filtered_fields # use default
        df = self.tab_man.get_df_cas(keepcodes='A|M|3',
                                     removecodes='R|1|2|4|5',
                                     event_fields=[])
        df = df.filter(col_list,axis=1)
        self.save_compressed_pickle(df,'filtered_pickle')
        self.save_compressed(df,'df_filtered_flat')

        
    def save_tables(self):
        """ save tables into the outdir.  If the outdir is the
        local ./out, save them as a single zip file; if it is
        ../result (CodeOcean operation), save directly into that
        folder as distinct files. CodeOcean doesn't facilitate zipping
        into a folder."""
        # save all the tables in the table manager to a zip file
        if self.outdir=='../results/':
            print('   ** making set of table files **')
            for tab in self.tab_man.tables.keys():
                df = self.tab_man.tables[tab].get_df()
                fn = self.outdir+'table_'+tab+'.csv'
                df.to_csv(fn,index=False)
        else:    
            print('   ** making "all_tables.zip" set **')
            tdir = self.tmpdir+'tables/'
            shutil.rmtree(tdir,ignore_errors=True)
            os.mkdir(tdir)
            
            with zipfile.ZipFile(self.outdir+'all_tables.zip','w') as z:
                for tab in self.tab_man.tables.keys():
                    df = self.tab_man.tables[tab].get_df()
                    fn = tdir+tab+'.csv'
                    df.to_csv(fn,index=False)
                    z.write(fn,compress_type=zipfile.ZIP_DEFLATED)
            shutil.rmtree(tdir,ignore_errors=True) #clean up

    def make_all_sets(self):
        """This takes a while because of the size of the data set and that
        we are compressing into zip files"""
        self.save_full_set()
        self.save_filtered_set()
        self.save_tables()
        
