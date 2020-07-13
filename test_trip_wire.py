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
# =============================================================================
# t = const_set.Construct_set(fromScratch=True,
#                             zfilename=currfn,
#                             sources=sources,
#                             outdir=outdir,
#                             tempfolder=tempfolder,
#                             stfilename=skyfn).get_quick_set()
# df = t.get_df_cas(keepcodes='',removecodes='')
# told = const_set.Construct_set(fromScratch=True,
#                         zfilename=lastfn,
#                         sources=sources,
#                         outdir=outdir,
#                         tempfolder=tempfolder,
#                         stfilename=skyfn).get_quick_set()
# olddf= told.get_df_cas(keepcodes='',removecodes='')
# 
# =============================================================================

tw.runTripWire(currfn,lastfn)