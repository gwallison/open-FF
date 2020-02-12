# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison

This script performs the overall task of creating a FracFocus database from
the raw excel collection and creating the output data sets.

Change the file handles at the top of this code to appropriate directories.
    
"""
#### -----------   File handles  -------------- ####

####### uncomment below for local runs
outdir = './out/'
sources = './sources/'
tempfolder = './tmp/'
make_files = True
### uncomment below for running on CodeOcean
#outdir = '../results/'
#sources = '../data/'
#tempfolder = '../'


####### zip input files
zfilename = 'currentData'
stfilename = 'sky_truth_final'


#### ----------    end File Handles ----------  ####

import shutil
import os
import core.Read_FF as rff
import core.Table_manager as c_tab
import core.Clean_event as clean_ev
import core.Clean_allrec as clean_ar
import core.Categorize_records as cat_rec
import core.Process_mass as proc_mass
import core.Generate_composite_fields as gen_fields
import core.Add_external_datasets as aed
import core.Make_working_sets as mws


class Construct_set():
    def __init__(self, fromScratch=False,zfilename=zfilename,
                 stfilename=stfilename,tempfolder=tempfolder,
                 sources=sources,outdir=outdir,
                 make_files=make_files):

        self.outdir = outdir
        self.sources = sources
        self.tempfolder = tempfolder
        self.processFromScratch=fromScratch
        self.zfilename = self.sources+zfilename+'.zip'
        self.stfilename = self.sources+stfilename+'.zip'
        self.make_files=make_files
        self.picklefolder = self.tempfolder+zfilename+'_pickles/'

    def initialize_dir(self,dir):
        shutil.rmtree(dir,ignore_errors=True)
        os.mkdir(dir)
                      
        
    def _banner(self,text):
        print()
        print('*'*50)
        space = ' '*int((50 - len(text))/2)
        print(space,text,space)
        print('*'*50)
        
    def get_full_set(self):
        tab_const = c_tab.Construct_tables(pkldir=self.picklefolder)
        if self.processFromScratch:
            self.initialize_dir(self.picklefolder)
            self._banner('PROCESS RAW DATA FROM SCRATCH')
            self._banner('Read_FF')
            raw_df = rff.Read_FF(zname=self.zfilename,
                                 skytruth_name=self.stfilename,
                                 outdir = self.outdir,
                                 gen_raw_stats=True).import_all()

            self._banner('Table_manager')
            raw_df = tab_const.add_indexes_to_full(raw_df)
            tab_const.build_tables(raw_df)
            tab_const.pickleAll(tmp=self.tempfolder)
            tab_const.listTables()

            self._banner('Clean_event')
            clean_ev.Clean_event(tab_manager=tab_const,
                                 sources=self.sources).process_events()

            self._banner('Clean_allrec')
            clean_ar.Clean_allrec(tab_manager=tab_const,
                                  sources=self.sources).process_records()

            self._banner('Generate_composite_fields')
            gen_fields.Gen_composite_fields(tab_manager=tab_const).make_infServiceCo()

            self._banner('Categorize_CAS')
            cat_rec.Categorize_CAS(tab_manager=tab_const,
                                   sources=self.sources,
                                   outdir=self.outdir).do_all()

            self._banner('Process_mass')
            proc_mass.Process_mass(tab_const).run()

        
            self._banner('Add_External_datasets')
            aed.add_Elsner_table(tab_const,sources=self.sources,outdir=self.outdir)
            aed.add_TEDX_ref(tab_const,sources=self.sources,outdir=self.outdir)
            aed.add_TSCA_ref(tab_const,sources=self.sources,outdir=self.outdir)


            self._banner('pickle all tables')
            tab_const.pickleAll()

            raw_df = None
            
            if self.make_files:
                self._banner('Make_working_sets')
                mws.Make_working_sets(tab_const,outdir=self.outdir,
                                      tmpdir=self.tempfolder).make_all_sets()
        else: 
            print('loading from pickles...')
            tab_const.loadAllPickles()
            tab_const.listTables()
        return tab_const

    def get_quick_set(self):
        """ generates a set of mostly of raw values - used mostly
        in pre-process screening. """
        
        tab_const = c_tab.Construct_tables(pkldir=self.picklefolder)
        if self.processFromScratch:
            raw_df = rff.Read_FF(zname=self.zfilename,
                                 skytruth_name=self.stfilename,
                                 gen_raw_stats=False).import_all()
            raw_df = tab_const.add_indexes_to_full(raw_df)
            tab_const.build_tables(raw_df)
            raw_df = None
        else: 
            print('loading from pickles...')
            tab_const.loadAllPickles()
            tab_const.listTables()
        return tab_const
