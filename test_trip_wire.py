# -*- coding: utf-8 -*-
"""
Created on Sat Jun 27 16:47:00 2020

@author: Gary
"""

import core.Construct_set as const_set
import core.trip_wire as tw
import pandas as pd

sources = './sources/'
outdir = './out/'
tempfolder = './tmp/'
skyfn = 'sky_truth_final'

currfn = 'testData_TW.zip'
lastfn = 'testData_last_TW.zip'
#lastfn = 'testData.zip'
#lastfn = 'currentData.zip'
#lastfn = 'ff_archive_2020-07-24.zip'
#lastfn = 'ff_archive_2020-03-20.zip'


df = tw.runTripWire(currfn,lastfn,usedate='testing')

#tw.singleCompare(currfn,lastfn,apis=['42329438480000',
#                                     '42329438560000',
#                                     '42329438550000',
#                                     '42329438490000'])

#tw.compareByClosest(currfn,lastfn,apis=['42329438480000'],
#                    dropcols=['UploadKey'])

#tw.process_archive(fnindex=0)