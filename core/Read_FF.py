
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:15:03 2019

@author: GAllison

This module is used to read all the raw data in from a FracFocus excel zip file

Input is simply the name of the archive file and it is typically passed
by the 'build_database' module.

All variables are read into the 'final' dataframe unless limited with the
"keep_list" input, althought that is currently all fields.

Re-typing and other processing is performed downstream in other modules.

This version also reads a pre-processed database of the SkyTruth set.

"""
import zipfile
import re
import csv
import pandas as pd
import numpy as np
import core.FF_stats as ffstats


class Read_FF():
    
    def __init__(self,zname='./sources/currentData.zip',
                 skytruth_name='./sources/sky_tryth_final.zip',
                 outdir = './out/',
                 tab_const=None,gen_raw_stats=False):
        self.zname = zname
        self.stname = skytruth_name
        self.outdir = outdir
        self.gen_raw_stats = gen_raw_stats
        self.dropList = ['ClaimantCompany', 'DTMOD', 'DisclosureKey', 
                         'IngredientComment', 'IngredientMSDS',
                         'IsWater', 'PurposeIngredientMSDS',
                         'PurposeKey', 'PurposePercentHFJob', 'Source', 
                         'SystemApproach'] # not used, speeds up processing
        
    def import_raw(self):
        """
        """
        dflist = []
        with zipfile.ZipFile(self.zname) as z:
            inf = []
            for fn in z.namelist():
                # the files in the FF archive with the Ingredient records
                #  always start with this prefix...
                if fn[:17]=='FracFocusRegistry':
                    # need to extract number of file to correctly order them
                    num = int(re.search(r'\d+',fn).group())
                    inf.append((num,fn))
                    
            inf.sort()
            infiles = [x for _,x in inf]  # now we have a well-sorted list
            for fn in infiles: #[-2:]:
                with z.open(fn) as f:
                    print(f' -- processing {fn}')
                    t = pd.read_csv(f,low_memory=False,
                                    #nrows=1000,
                                    dtype={'APINumber':'str'}
                                    # ignore pandas default_na values
#                                    keep_default_na=False,na_values='')
                                    )
                    # we need an indicator of the presence of IngredientKey
                    # whitout keeping the whole honking thing around
                    t['ingKeyPresent'] = np.where(t.IngredientKey.isna(),
                                                  False,True)
                    t['raw_filename'] = fn
                    t['record_flags'] = 'B'  #bulk download flag (in allrec)
                    t['data_source'] = 'bulk' # for event table
                    
                    dflist.append(t)
        final = pd.concat(dflist,sort=True)
        return final
        
    def import_raw_as_str(self,varsToKeep=['UploadKey','APINumber',
                                           'IngredientName','CASNumber',
                                           'StateName','StateNumber',
                                           'CountyName','CountyNumber',
                                           'FederalWell','IndianWell',
                                           'JobStartDate','JobEndDate',
                                           'Latitude','Longitude',
                                           'MassIngredient','OperatorName',
                                           'PercentHFJob','PercentHighAdditive',
                                           'Purpose','Supplier','TVD',
                                           'TotalBaseWaterVolume','TotalBaseNonWaterVolume',
                                           'TradeName','WellName',
                                           'IngredientKey']):
        """
        """
        dflist = []
        with zipfile.ZipFile(self.zname) as z:
            inf = []
            for fn in z.namelist():
                # the files in the FF archive with the Ingredient records
                #  always start with this prefix...
                if fn[:17]=='FracFocusRegistry':
                    # need to extract number of file to correctly order them
                    num = int(re.search(r'\d+',fn).group())
                    inf.append((num,fn))
                    
            inf.sort()
            infiles = [x for _,x in inf]  # now we have a well-sorted list
            dtypes = {}
            for v in varsToKeep:
                dtypes[v] = 'str'
            for fn in infiles:
                with z.open(fn) as f:
                    print(f' -- processing {fn}')
                    t = pd.read_csv(f,low_memory=False,
                                    usecols=varsToKeep,
                                    dtype=dtypes,
                                    na_filter=False,
                                    # ignore pandas default_na values
#                                    keep_default_na=False,na_values='')
                                    )
                    # we need an indicator of the presence of IngredientKey
                    # whitout keeping the whole honking thing around
#                    t['ingKeyPresent'] = np.where(t.IngredientKey.isna(),
#                                                  False,True)
#                    t['raw_filename'] = fn
#                    t['record_flags'] = 'B'  #bulk download flag (in allrec)
#                    t['data_source'] = 'bulk' # for event table
                    
                    dflist.append(t)
        final = pd.concat(dflist,sort=True)
        return final

    def import_skytruth(self):
        """
        This function pulls in a pre-processed file with the Skytruth data.
        The pre-processing reformated the Skytruth data to match the FracFocus
        bulk download format, to allow merging.  Note, however, that we do NOT
        link the FF 'placeholder' events for FFVersion 1 to these skytruth data
        Those place holders are essentially removed from the working set
        because they have no chemical records associated.  While those
        placeholders have metadata that is important, we will rely on the Skytruth
        versions of that metatdata (which should be identical).
        
        The pdfs from which skytruth scraped only reported 10 digits in the 
        APINumber field.  However, the bulk download reports 14 digits. So for
        the output of this function, we append four X's to fill out the numbers.
        It may make sense to get that piece of metadata from the bulk download.
        
        """
        with zipfile.ZipFile(self.stname) as z:
            fn = z.namelist()[0]
            with z.open(fn) as f:
                print(f' -- processing {fn}')
                t = pd.read_csv(f,low_memory=False,
                                quotechar='$',quoting=csv.QUOTE_ALL,
                                dtype={'APINumber':'str'},
                                #nrows = 1000, # Uncomment this line for faster running
                                # Use pandas default_na values; dealt with in pre-processing
                                keep_default_na=True)
                t['raw_filename'] = 'SkyTruth'
                t['record_flags'] = 'Y'  #skytruth flag
                t['data_source'] = 'SkyTruth'
                t.APINumber = np.where(t.APINumber.str.len()==13, #shortened state numbers
                                       '0'+ t.APINumber,
                                       t.APINumber)
                t.APINumber = np.where(t.APINumber.str.len()==9, #shortened state numbers
                                       '0'+ t.APINumber + 'XXXX',
                                       t.APINumber)
                t.APINumber = np.where(t.APINumber.str.len()==10,
                                       t.APINumber + 'XXXX',
                                       t.APINumber)
                t['ingKeyPresent'] = True  # all SkyTruth events have chem records
        return t

    def import_all(self):
        t = pd.concat([self.import_raw(),
                       self.import_skytruth()],
                       sort=True)
        t.reset_index(drop=True,inplace=True) #  single integer as index
        t['reckey'] = t.index.astype(int)
        if self.gen_raw_stats:
            print('  --- calculating stats on raw data')
            ffstats.FF_stats(t,outfn=self.outdir+'ff_raw_stats.txt').calculate_all()
           
        t.drop(columns=self.dropList,inplace=True)
        assert(len(t)==len(t.reckey.unique()))
        return t