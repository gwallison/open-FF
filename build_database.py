# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison

This script performs the overall task of creating a FracFocus database from
the raw excel collection and creating the tables used to make data sets.

Change the file handles at the top of core.Construct_set to point to appropriate
directories.

    
"""
processFromScratch = True
make_output_files = True

import core.Construct_set as const_set


t = const_set.Construct_set(fromScratch=processFromScratch,
                            make_files=make_output_files).get_full_set()

