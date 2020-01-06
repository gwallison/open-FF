
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

#!!! Need to read in APINumber as string. and correct SKYTruth APINumber so that
#it is 14 digits (merge with placeholders?)

class Read_FF():
    
    def __init__(self,zname='./sources/currentData.zip',
                 skytruth_name='./sources/sky_tryth_final.zip',
                 tab_const=None):
        self.zname = zname
        self.stname = skytruth_name
        self.dropList = ['ClaimantCompany', 'DTMOD', 'DisclosureKey', 
                         'IngredientComment', 'IngredientMSDS',
                         'IsWater', 'Projection', 'PurposeIngredientMSDS',
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
            for fn in infiles:
                with z.open(fn) as f:
                    print(f' -- processing {fn}')
                    t = pd.read_csv(f,low_memory=False,
                                    dtype={'APINumber':'str'}
                                    # ignore pandas default_na values
#                                    keep_default_na=False,na_values='')
                                    )
                    # we need an indicator of the presence of IngredientKey
                    # whitout keeping the whole honking thing around
                    t['ingKeyPresent'] = np.where(t.IngredientKey.isna(),
                                                  False,True)
                    t['raw_filename'] = fn
                    
                    dflist.append(t)
        final = pd.concat(dflist,sort=True)
        #final[final.UploadKey=='30761a0d-b09d-4bac-bb38-129cad245872'].to_csv('./tmp/temp.csv')
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
                t.APINumber = t.APINumber + 'XXXX'
                t['ingKeyPresent'] = True  # all SkyTruth events have chem records
                #  create a basic integer index for easier reference
        return t

    def import_all(self):
        t = pd.concat([self.import_raw(),
                       self.import_skytruth()],
                       sort=True)
        t.reset_index(drop=True,inplace=True) #  single integer as index
        t['reckey'] = t.index.astype(int)
        t.drop(columns=self.dropList,inplace=True)
        assert(len(t)==len(t.reckey.unique()))
        #print(f'len of df: {len(t)} ---- unique reckey: {len(t.reckey.unique())}')
        return t