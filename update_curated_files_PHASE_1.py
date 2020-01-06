# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 09:12:36 2019

@author: Gary

This is code used to update curated files when a new version of the FF
data is to be processed. 

In phase 1, we read in all the data, and identify any valid CAS numbers
that have not been seen before.  The output of this code is the file 
/tmp/cas_to_check.csv.  If you run this code and that file has no entries,
you are ready to move on to phase 2.

However, if there are entries in cas_to_check.csv, follow these steps:
    1) run all the cas numbers in that file through a check at SciFinder.com
       in their 'Substance Identifier' (you can run up to 25 cas numbers
       at a time).  If a given search returns one or more results (that is, valid
       cas materials), select all of them, choose, export, and save the export
       results as a 'quoted text' file.  Save that file and add it to 
       the /sources/CAS_ref_files/folder within the CAS_ref project (a different
       folder altogether) with all the other SciFinder results. 
       
       You must then
       run the process_CAS_ref_files.py code and move the resulting file,
       CAS_ref_and_names.csv to the main sources directory.

       This step expands the program's catelog of authoritative cas numbers so 
       that when we run the main code, the new cas numbers will be recognized
       and properly identified.

    2) It sometimes happens that a cas number in FF is valid (at least in format)
       but it does not reference an actual chemical.  We keep list of those
       in /sources/valid_but_empty.csv, so that we do not have to run them
       through SciFinder every time we update the bulk data set.  To find those
       and save them in that csv file, just run this code 
       (update_curated_files_PHASE_1.py)  again.  If you have transfered all the
       exported files to the CAS_ref directory (step 1 above), all the entries
       in cas_to_check.csv should be spurious and can be added to the 
       valid_but_empty file.  (Careful with this though, once you move a number
       into the valid_but_empty file, it won't be examined again and all records
       with that number are essentially ignored!  You can safely skip this step
       if you don't mind checking those empty numbers in SciFinder every time you
       update the data.)
       
       Another potential issue at this stage is that there are some cas numbers
       used in FF that are considered obsolete and the material is actually
       widely named with a different cas number.  The original and its replacement
       should go into the file sources/cas_to_rename.csv
       
After these steps, you can move on to Phase 2.
"""
import core.Construct_set as const_set
import updating.make_updated_xlate as mux


tab_const = const_set.Construct_set(fromScratch=True).get_full_set()

mux.check_for_new_cas(tab_manager=tab_const)