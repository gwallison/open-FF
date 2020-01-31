README for FF-POC repository and project

This CodeOcean capsule is an extension of the Proof of 
Concept version of code to transform the online chemical disclosure site 
for hydraulic fracturing, FracFocus.org, into a usable database.  

The code performs cleaning, filtering, and 
curating techniques to yield organized data sets and sample analyses 
from a notoriously messy collection of chemical records.   
The sample analyses are available in the results section as 
downloadable versions of the final data.  
For a majority of the records, the mass of the chemicals is calculated. 

To be included in final data sets, 
   Fracking events must use water as carrier and percentages must be 
     consistent and within tolerance.
   Chemicals must be identified by a match with an authoritative CAS number 
     or be labeled proprietary.

Further, portions of the raw bulk data that are filtered out include: 
- fracking events with no chemical records (mostly 2011-May 2013; but in
  this version, are replaced with the SkyTruth archive).
- fracking events with multiple entries (and no indication which entries 
    are correct).
- chemical records that are identified as redundant within the event.

Finally,  I clean up some of the labeling fields by consolidating multiple 
versions of a single category into an easily searchable name. For instance, 
I collapse the 80+ versions of the supplier 'Halliburton' to a single name.

By removing or cleaning the difficult data from this unique data source, 
I produce a data set that should facilitate more in-depth 
analyses of chemical use in the fracking industry.


Location assumptions: Several FF fields help identify the location of a fracking site:
StateName, CountyName, Latitude, Longitude, StateNumber, CountyNumber
Unfortunately, often these fields are not consistent. For example, the StateName may be
Texas, but the StateNumber indicates Oklahoma. County Names are frequently wrong, though
typically the names given are nearby counties.

In trying to reconcile this, we assume that
a well's APINumber is correct, and as long as the StateNumber and CountyNumber correspond 
to that number (they are embedded in the APINumber), we use those as the source of the 
bgStateName and bgCountyName. When the Lat/Lon numbers in FF are too far from an indicated
county, we assume they are wrong and use the center of the county as the georeference.

******  open-FF  Version explanation ******

Version 1: Data downloaded from FracFocus on Jan. 22, 2020.  SkyTruth archive
   has been incorporated.  Links to references include: Elsner & Hoelzer 2016, 
   TEDX chemical list and TSCA list.


