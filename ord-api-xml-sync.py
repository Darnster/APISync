import urllib.request
import xml.etree.ElementTree as ET
import datetime, sys, re, os

'''

Title: ORD API XML Synchroniser
Compatibility: Python 3.6 or later
ORD XML Schema version: v2-0-0
Status: Draft
Author: NHS Digital
Contact: exeter.helpdesk@nhs.net
Release Date: 2018-11-20
Project: ORD Changes (HSCOrgRefData)
Internal Ref: v0.1
Copyright Health and Social Care Information Centre (c) 2018

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


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
 
'''

class APIRetrieve(object):

    def __init__(self):
        '''initialise class vars here '''

        self.tree = ""
        self.root = ""

        # generate Date and DateTime in the ISO format
        self.sysDate = datetime.datetime.now().strftime("%Y-%m-%d") # ISO date
        self.sysTime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z") # Schema compliant format
        self.fileTime = datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S%Z") # can't include ":" in filenames

        # temp location to store data retrieved from the API
        self.tempOrgListFile = ".\\orglist.xml"
        self.headerCodeSystemFile = "HeaderCodeSystems.txt"
        self.outputFile = "APISyncFile_%s.xml" % self.fileTime
        self.logFileName = "APILogFile.log"
        self.recordCount = 0

        #exception messages print
        self.ex = APIException()

        # define the progress that will be reported to the console
        self.progressMarkers = [10, 25, 40, 50, 65, 80, 95, 100]

    def getSyncData(self, apiUri, LastChangeDate):
        '''
        call to API and store results in temporary tempOrgListFile.xml
        '''
        # Open the log file handler
        self.lh = APILog(self.logFileName)


        # log method entry
        self.lh.write("getSyncData called")

        # Validate LastChangeDate against ISO date format e.g. 2018-10-15
        pat = re.compile("^([0-9]{4})-?(1[0-2]|0[1-9])-?(3[01]|0[1-9]|[12][0-9])$")
        if not re.match(pat, LastChangeDate):
            message = "Please check the date provided %s" % LastChangeDate
            print( message )
            self.lh.write( message )
            sys.exit()
        self.apiCall = '%s?LastChangeDate=%s&_format=xml' % (apiUri, LastChangeDate)
        self.lh.write("getSyncData successful")
        try:

            self.tempOrgListFile, self.headers = urllib.request.urlretrieve(self.apiCall)

        except urllib.error.URLError as e:
            self.ex.printException(e)
            self.lh.write("getSyncData exception %s" % e.reason)
            self.rollBack(e.reason, "ignore")
            sys.exit()

        except urllib.error.HTTPError as e:
            self.ex.printException(e)
            self.lh.write("getSyncData exception %s" % e.reason)
            self.rollBack(e.reason, "ignore")
            sys.exit()


        except OSError.TimeoutError as e:
            self.ex.printException(e)
            self.lh.write("writeToFile exception %s" % e.reason)
            self.rollBack(e.reason, "ignore")
            sys.exit()

        # get the number of records returned by the query
        self.recordCount = self.headers.get('X-Total-Count')

        # Read the file into memory
        self.tree = ET.parse(self.tempOrgListFile)
        self.root = self.tree.getroot()

        '''
        It looks like this
        <Organisations>
            <Organisation>
                <OrgLink>
                    https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/V81871?_format=xml
                </OrgLink>
            </Organisation>
        .....
        </Organisations>
        '''

    def getRecordCount(self):
        '''
        Returns the Recordcount from HTTP Headers
        :return RecordCount as a string:
        '''
        #print(self.recordCount)
        self.lh.write("getRecordCount called")
        return self.recordCount


    def writeToFile(self):
        '''Read the header and CodeSystems template and then append the retrieved data
        Steps here are:
        1. open the template file HeaderCodeSystems.txt
        2. Replace any instances of $DateTime and $Date with sys date and sys time, populate $records
        3. Write the template text to a new File APISyncFile_<filedate>.xml
        4. Loops over all records returned from the API and writes those to the file
        5. Appends closing XML content for the tail of the file
        '''

        self.lh.write("writeToFile called")

        recordCount = self.getRecordCount() # call once here to avoid calling the functions for the replace commands below
        self.lh.write("getRecordCount successful %s records in batch" % recordCount)

        if recordCount == "0":
            # No need to do all this stuff
            message = "%s didn't return any results" % self.apiCall
            print( message )
            self.lh.write( message )
            sys.exit()
        message = "writeToFile - reading template data from %s" % self.headerCodeSystemFile
        print( message )
        self.lh.write( message )
        #1 open template file
        output = open(self.outputFile, "w", newline = '')
        try:
            headerLines = open(self.headerCodeSystemFile, "r")
        except FileNotFoundError:
            # can't pass built-in exception to APIException() so print here instead
            message = "writeToFile exception - FileNotFound %s" % self.headerCodeSystemFile
            print(message)
            self.lh.write(message)
            self.rollBack(message, output)
            sys.exit()

        #2 and #3 Write to the output file with substitutions in the manifest
        message = "writeToFile - writing template manifest and codesystems data to %s" % self.outputFile
        print( message )
        self.lh.write( message )
        for line in headerLines:
            line.strip() # ditch any whitespace
            line = line.replace('$DateTime', self.sysTime)
            line = line.replace('$Date', self.sysDate)
            line = line.replace('$Records', recordCount)
            output.write(line)
        message = "writeToFile - template manifest and codesystems data written to %s" % self.outputFile
        print( message )
        self.lh.write( message )
        # begin payload
        message = "writeToFile - writing XML payload records to %s" % self.outputFile
        print( message )
        self.lh.write( message )

        output.write("<Organisations>")

        #4 loop over returned records
        recordCounter = 0
        for elem in self.root:
            for org_list in elem:
                # example of orglist.text "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/V81871?_format=xml"
                try:

                    with urllib.request.urlopen(org_list.text) as response:
                        xml_record = str(response.read())
                        '''
                        The first 44 chars of each xml_record is the xml root declaration and need to be discarded, e.g.
                        '<?xml version=\'1.0\' encoding=\'UTF-8\'?>
                        There is also a single quote at the end that needs removing, hence  -1 in the string indexing below
                        '''
                        xml_record = xml_record[44:-1]
                        output.write('%s\n' % xml_record)
                        recordCounter += 1
                        self.reportProgress(recordCounter, recordCount)

                except urllib.error.URLError as e:
                    self.ex.printException(e)
                    self.lh.write("writeToFile exception %s" % e.reason)
                    self.rollBack(e.reason, output)
                    sys.exit()

                except urllib.error.HTTPError as e:
                    self.ex.printException(e)
                    self.lh.write("writeToFile exception %s" % e.reason)
                    self.rollBack(e.reason, output)
                    sys.exit()

                except OSError.TimeoutError as e:
                    self.ex.printException(e)
                    self.lh.write("writeToFile exception %s" % e.reason)
                    self.rollBack(e.reason, output)
                    sys.exit()

        #5 append closing elements
        output.write("</Organisations>\n")
        output.write("</HSCOrgRefData:OrgRefData>\n")

        # close the output file
        output.close()
        message = "writeToFile -  complete %s records written successfully to %s" % ( recordCounter, self.outputFile )
        print( message )
        self.lh.write( message )
        self.lh.close()

    def reportProgress(self, recordCounter, recordCount):
        '''Simply prints out an indication of progress'''
        percentage = int(recordCounter / int(recordCount) * 100)
        if percentage in self.progressMarkers:
            print("%s percent of records written" % percentage)
            self.progressMarkers.pop(0)


    def rollBack(self, message, output):
        '''remove any output file if an error occurs'''
        if output != "ignore":  # for exceptions raised before the output file is created
            # log the action
            self.lh.write( "Process rolled back - message = %s" % message )
            #try:
            # close the output file
            output.close()
            # delete the output file
            fileToDelete = os.path.join(os.getcwd(), self.outputFile)
            os.remove( fileToDelete )
            self.lh.write("%s has been removed" % self.outputFile )
        else:
            pass



class APIException():
    '''Only responsible for printing the details of exceptions raised'''
    def __init__(self):
        pass

    def printException(self, e):
        print("%s\n" % e.reason)
        print("General Information for this script (may not be related to this exception): only calls to the sync endpoint are supported with LastChangeDate <=sysdate - 190 days and the data is only returned as xml.")


class APILog():
    '''Logs the progress of the script'''
    def __init__(self, logfile):
        '''Creates the file if it doesn't exist'''
        self.logFile = open(logfile, "a")

    def write(self, s):
        logTime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z")
        self.logFile.write("%s,%s\n" % ( logTime, s ) )

    def close(self):
        self.logFile.close()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print("Command Line Arguments:")
        for x in sys.argv:
            print("\t%s" % x)
        urlAPI = sys.argv[1]
        LastChangeDate = sys.argv[2]
        # Validate LastChangeDate against ISO date format e.g. 2018-10-15
        pat = re.compile("^([0-9]{4})-?(1[0-2]|0[1-9])-?(3[01]|0[1-9]|[12][0-9])$")
        if not re.match(pat, LastChangeDate):
            print("Please check the date provided %s" % LastChangeDate)
            sys.exit()

        ar = APIRetrieve()
        ar.getSyncData(urlAPI, LastChangeDate)
        print("Records found = %s" % ar.getRecordCount())
        ar.writeToFile()
    else:
        print("Please provide the following arguments:\nURL for the API Service\nLastChangeDate")

    """
    Example call = python ord-api-xml-sync.py https://directory.spineservices.nhs.uk/ORD/2-0-0/sync 2018-11-15
    """
