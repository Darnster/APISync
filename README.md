# APISync

WHAT THIS CODE DOES:
The classes defined below allow those customers who have interfaced with ODS XML to retrieve records from the ODS API Suite and 
render them into an XML file which conforms to the ORD schema.
The script calls the sync endpoint of the ORD Interface within the ODS API Suite (https://directory.spineservices.nhs.uk/ORD/2-0-0/sync) 
and passes in <LastChangeDate> and "_format=xml" in order to retrieve individual XML records.  It then generates a schema compliant 
XML file which can be passed to the XSLT tools provided by ODS for further processing (e.g. XMLtoCSV or Primary Role Transform).
The header, manifest and CodeSystems content is held in a static file called HeaderCodeSystems.txt.  The text from the file is
written to the output file and the manifest is updated with Date/DateTime values based on sysdate; the RecordCount is populated
to match the volumes returned.  
*** Note that the CodeSystems specified in the template file (HeaderCodeSystems.txt) will need to be updated periodically from the
full XML file in order to pick up changes to Roles, Relationships and RecordClass definitions to ensure that the output can be 
processed via the XSLT tools. ***
All records returned from the method getSyncData() are written to the output file via the method writeToFile(), topped and tailed 
with the headers and CodeSystems content from the template file plus the closing XML elements </Organisations> and 
</HSCOrgRefData:OrgRefData>.
LOGGING
All stages of the sync process are logged to a file called "APILog.log"
EXCEPTION HANDLING
Note that if an exception is identified at any point, this is logged to "APILogFile.log" and the output file is removed via a
rollback function to ensure an inconsistent file isn't picked up by other processes.
*** Only XML content is returned by this software ***

Author: Danny Ruttle
