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

### uncomment below for running on CodeOcean
#outdir = '../results/'
#sources = '../data/'
#tempfolder = '../results/'


####### zip input files
zfilename = sources+'currentData.zip'
stfilename = sources+'sky_truth_final.zip'

####### output
raw_stats_fn = outdir+'ff_raw_stats.txt'

#### ----------    end File Handles ----------  ####

import core.Read_FF as rff
import core.Table_manager as c_tab
import core.Clean_event as clean_ev
import core.Clean_allrec as clean_ar
import core.Categorize_records as cat_rec
import core.Process_mass as proc_mass
import core.Add_external_datasets as aed


class Construct_set():
    def __init__(self, fromScratch=False,zfilename=zfilename,
                 stfilename=stfilename,tempfolder=tempfolder,
                 sources=sources,outdir=outdir):

        self.outdir = outdir
        self.sources = sources
        self.tempfolder = tempfolder
        self.processFromScratch=fromScratch
        self.zfilename = zfilename
        self.stfilename = stfilename
        
    def _banner(self,text):
        print()
        print('*'*50)
        space = ' '*int((50 - len(text))/2)
        print(space,text,space)
        print('*'*50)
        
    def get_full_set(self):
        tab_const = c_tab.Construct_tables()
        if self.processFromScratch:
            self._banner('PROCESS RAW DATA FROM SCRATCH')
            self._banner('Read_FF')
            raw_df = rff.Read_FF(zname=self.zfilename,
                                 skytruth_name=self.stfilename).import_all()
            self._banner('Table_manager')
            raw_df = tab_const.add_indexes_to_full(raw_df)
            tab_const.build_tables(raw_df)
            tab_const.pickleAll(tmp=self.tempfolder)
            tab_const.listTables()
            #clean_loc.Clean_location(tab_manager=tab_const,sources=sources).process_location()
            self._banner('Clean_event')
            clean_ev.Clean_event(tab_manager=tab_const,
                                 sources=self.sources).process_events()
            #print(f'After clean_ev: {tab_const.tables["event"].df.columns}')
            self._banner('Clean_allrec')
            clean_ar.Clean_allrec(tab_manager=tab_const,
                                  sources=self.sources).process_records()
            #print(f'After clean_allrec: {tab_const.tables["event"].df.columns}')
            self._banner('Categorize_CAS')
            cat_rec.Categorize_CAS(tab_manager=tab_const,
                                   sources=self.sources,
                                   outdir=self.outdir).do_all()
            #print(f'After cat_cas: {tab_const.tables["event"].df.columns}')
            self._banner('Process_mass')
            proc_mass.Process_mass(tab_const).run()
            #print(f'After process_mass: {tab_const.tables["event"].df.columns}')
        
            self._banner('Add_External_datasets')
            aed.add_Elsner_table(tab_const,sources=self.sources,outdir=self.outdir)
            #print(tab_const.tables['cas'].df.columns)
            self._banner('pickle all tables')
            tab_const.pickleAll()
            #print(tab_const.tables['cas'].df.columns)
            raw_df = None
        else: 
            print('loading from pickles...')
            tab_const.loadAllPickles()
            tab_const.listTables()
        return tab_const

    def get_quick_set(self):
        tab_const = c_tab.Construct_tables()
        if self.processFromScratch:
            raw_df = rff.Read_FF(zname=self.zfilename,
                                 skytruth_name=self.stfilename).import_all()
            raw_df = tab_const.add_indexes_to_full(raw_df)
            tab_const.build_tables(raw_df)
            raw_df = None
        else: 
            print('loading from pickles...')
            tab_const.loadAllPickles()
            tab_const.listTables()
        return tab_const
