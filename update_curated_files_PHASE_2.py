# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 09:12:36 2019

@author: Gary

In this second phase of updating curated files, we add any new values to the
curated 'xlate' files.  This code simply looks the new data set and adds any
values that are not in the old curated xlate files to a new file (saved in /tmp. 
 The user then edits those files to make the necessary judgements about 
the added values and then saves that edited file in the /sources directory, 
replacing the old version.

One slight oddity: the files are csv files and some of the files are quoted 
with the '$' character, meaning that there is a $ at the beginning and end of 
every cell.  Most csv readers (excel, open office, for example) let you specify
what the quoting character is.  Once you do that, you don't have to worry about 
anything else - the program will eliminate the $'s while you edit, and will
put them back when you save it.  Other files (cas_values) use the more standard 
double-quote character and you probably won't have to think about it at all.

"""

import core.Construct_set as const_set
import updating.make_updated_xlate as mux

tab_const = const_set.Construct_set(fromScratch=False).get_full_set()
mux.gen_new_files(tab_manager=tab_const)