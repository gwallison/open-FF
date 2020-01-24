Some of the code in this directory is used when a newer FracFocus
data set is to be cleaned.  Because some of the cleaning
depends on the curated data (for example, what to do
with a specific CASNumber), and NEW codes encountered in the
new data set must be evaluated BY HAND before a new cleaned
database can be created, the steps below should be performed BEFORE you
use build_database.py

The steps to do the updating:

1) Download a new bulk data set from fracfocus and put it into the sources
directory.  Rename it 'currentData.zip' - (after you have removed or renamed
the old data set file).

## CAS Numbers ##

2) Run update_curated_files_PHASE_1.py from the main directory. This will
first give you an idea how many new records are in the new set.  It will then
produce a list of new CAS numbers that need to be evaluated.  That is in
the 'cas_to_check.csv' file.  If that has no new cas numbers, you can move on
to step 3.

- 2a) Search with the new values in cas_to_check.csv  at SciFinder to determine
if they are authoritative numbers or not.  Use the instructions in the PHASE_1
code description for more detailed information.  This may include downloading
data from Scifinder, running code to process these reference files and moving
a new reference file to the main sources area.

## Location corrections ##

3) Run the script 'update_location_scan.py'.  This code will validate and correct
location data for all new records.  This is pretty much a hands-off
process. 

4) Run update_curated_files_PHASE_2.py from the main directory. 
This code will create new CSV files in
the /tmp directory that will be named the same as the 
/source files but with the new codes waiting to be assigned.

5) These /tmp files should be curated by hand to incorporate 
the new codes.  Currently these files are:
  company_xlate.csv,
  cas_labels.csv
Those rows in these files that are already processed will have 'curated' in
the status column.  After you change the values, be sure to also change the
status.

6) Move the updated /tmp files into the /sources (or /data if in CodeOcean) 
directory, replacing the older versions.  

You may then proceed to the main task: build_database.py