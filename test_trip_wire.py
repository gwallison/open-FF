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

currfn = 'testData.zip'
lastfn = 'testData_last.zip'
#lastfn = 'testData.zip'
#lastfn = 'currentData.zip'
lastfn = 'ff_archive_2020-07-24.zip'
#lastfn = 'ff_archive_2020-03-20.zip'


df = tw.runTripWire(currfn,lastfn)

#tw.singleCompare(currfn,lastfn,apis=['42383406940000',
#                                     '42461409710000',
#                                     '35073265430000'])

#tw.process_archive(fnindex=0)