# README for open-FF repository and project

This CodeOcean capsule is an extension of the Proof of 
Concept version of code to transform data from the online chemical disclosure site 
for hydraulic fracturing, FracFocus.org, into a usable database.  

The code performs cleaning, flagging, and 
curating techniques to yield organized data sets and sample analyses 
from a difficult collection of chemical records.   
For a majority of the chemical records, the mass of the chemicals used 
in fracking operations is calculated. 

The output of this project includes full data sets and filtered data sets. All 
sets include many of the original raw FracFocus fields and many generated
fields that correct and add context to the raw data.  The full sets do not 
filter out any of the original raw FracFocus records but leaves that up to 
the user (by using the record_flags field, etc.)  Filtered data sets remove
the FracFocus records that have significant problems to give the user a 
product that is usable without much work.

To be included in filtered data sets, 
- Fracking events must use water as carrier and percentages must be 
consistent and within tolerance.
- Chemicals must be identified by a match with an authoritative CAS number 
or be labeled proprietary.

Further, portions of the raw bulk data that are filtered out include: 
- fracking events with no chemical records (mostly 2011-May 2013; but in
  this version, are replaced with the SkyTruth archive).
- fracking events with multiple entries (and no indication which entries 
    are correct).
- chemical records that are identified as redundant within the event.

Finally,  I clean up some of the labeling fields by consolidating multiple 
versions of a single category into an easily searchable name. For instance, 
I collapse the 80+ versions of the supplier name 'Halliburton' to a single
value 'halliburton'.

By removing or cleaning the difficult data from this unique data source, 
I hope I have produced a data set that should facilitate more in-depth 
analyses of chemical use in the fracking industry.

Location assumptions: Several FF fields help identify the location of a fracking site:
StateName, CountyName, Latitude, Longitude, StateNumber, CountyNumber.  
Unfortunately, often these fields are not consistent. For example, the StateName may be
Texas, but the StateNumber indicates Oklahoma. County Names are frequently wrong, though
typically the names given are nearby counties.

In trying to reconcile this, we assume that
a well's APINumber is correct, and as long as the StateNumber and CountyNumber correspond 
to that number (they are embedded in the APINumber), we use those as the source of the 
bgStateName and bgCountyName. When the Lat/Lon numbers in FF are too far from an indicated
county, we assume they are wrong and use the center of the county as the georeference.

## Versions of Open-FF 

**Version 9**: Correct FF_stats.py calculation for percent non-zero in the integer
    and float section. Generate geographic clusters as proxy of wellpad identity;
    clusters are found in the string field "clusterID". (Note that a specific clusterID will NOT be
    consistent across data set versions in the way that UploadKey is; don't depend
    on it!).  The fields FederalWell and IndianWell have been changed to string type -
    previously, they were boolean (T/F) but that type does not allow for empty
    cells which occurs in the SkyTruth data, leading to misinformation.
    Modified input SkyTruth APINumber to detect duplicate disclosures more completely.
    This results in many SkyTruth disclosures being flagged as duplicates.
    Data download from FracFocus on January 15, 2021.

**Version 8.1**: Correct slight documentation omission.

**Version 8**: Added WellName field to filtered data output.  Added chemical ingredient
   codes from the WellExplorer project (www.WellExplorer.org) -- fields with
   the prefix 'we_' are from that project. See https://doi.org/10.1093/database/baaa053
   Data download from FracFocus on October 23, 2020.

**Version 7**: Data downloaded from FracFocus on July 31, 2020.  TradeName added
   to exported data.

**Version 6**: Save data tables in pickled form into results section so that it may be
   exported to other projects.

**Version 5**: Data downloaded from FracFocus on May 14, 2020.  No other changes.

**Version 4**: Data downloaded from FracFocus on March 20, 2020.  Added the generated
   field, infServiceCo. This field is an attempt to identify the primary
   service company of a fracking event.  Including in the output files the 
   raw field 'Projection' which is needed to accurately map using lat/lon
   data.

**Version 3**: Data downloaded from FracFocus on Jan. 22, 2020. Modified the 
   FF_stats module to generate separate reports for the "bulk download" and
   the "SkyTruth" data sources.  Both are reported in "ff_raw_stats.txt" in the
   results section.)

**Version 2**: Data downloaded from FracFocus on Jan. 22, 2020. Incorporated 
   basic statistics on the raw FracFocus data (see "ff_raw_stats.txt" in the
   results section.)

**Version 1**: Data downloaded from FracFocus on Jan. 22, 2020. Similar 
   to the Proof-of-Concept version with the following new features:
   SkyTruth archive
   has been incorporated.  Links to references include: Elsner & Hoelzer 2016, 
   TEDX chemical list and TSCA list.


