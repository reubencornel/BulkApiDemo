
#!/usr/bin/env python
from httplib import HTTPSConnection 
from httplib import HTTPConnection
import urllib
import re
import xml.etree.ElementTree as ET

def findNode(children, tag):
    for child in children:
        if (child.tag == tag):
            return child.text
    return None


class SalesforceLogin:
    def __init__(self, username, password, host):
        self.userName = username
        self.password = password
        self.errorMessage = ""
        self.body = '''<?xml version="1.0" encoding="utf-8" ?>
<env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
  <env:Body>
    <n1:login xmlns:n1="urn:partner.soap.sforce.com">
      <n1:username>USERNAME</n1:username>
      <n1:password>PASSWORD</n1:password>
    </n1:login>
  </env:Body>
</env:Envelope>'''
        self.sessionId = ''
        self.authenticated = False

        if (isinstance(host, HTTPConnection) or isinstance(host, HTTPSConnection)):
            self.host = host
        else:
            self.host = HTTPSConnection(host)

    def getUsername(self):
        return self.userName

    def getPassword(self):
        return self.password
        
    def getSessionId(self):
        return self.sessionId

    def setSessionId(self, sessionId):
        self.sessionId = sessionId
        self.setAuthenticated(True)

    def login(self):
        headers = {'Content-Type' : 'text/xml; char=UTF-8', "SOAPAction": "login"}
        self.body = re.sub("USERNAME", self.userName, self.body)
        self.body = re.sub("PASSWORD", self.password, self.body)
        self.host.request("POST", "/services/Soap/u/35.0", self.body, headers)
        response = self.host.getresponse()
        root = ET.fromstring(response.read())
        if (response.status == 200):
            self.sessionId = self.parseResponse(root)
            print ("Session Id: " + self.sessionId)
            self.authenticated = True
        else:
            self.errorMessage = self.parseErrorResponse(root)
            self.authenticated = False

        self.host.close()

    def parseResponse(self, root):
        return findNode(root.getchildren()[0].getchildren()[0].getchildren()[0].getchildren(), '{urn:partner.soap.sforce.com}sessionId')

    def parseErrorResponse(self, root):
        return findNode(root.getchildren()[0].getchildren()[0].getchildren(), 'faultstring')


    def isAuthenticated(self):
        return self.authenticated

    def setAuthenticated(self, auth):
        self.authenticated = auth

    def getMessage(self):
        return self.errorMessage
            
class BulkOperation:
    jobId = ''
    def __init__(self, username, password, host, salesforcelogin, endpoint="/services/async/35.0/"):
        if (salesforcelogin == None):
            self.session = SalesforceLogin(username, password, host)
            self.session.login();
        else:
            self.session = salesforcelogin

        if (isinstance(host, HTTPConnection) or isinstance(host, HTTPSConnection)):
            self.host = host
        else:
            self.host = HTTPSConnection(host)

        self.endpoint = endpoint
        self.concurrencyMode = 'Parallel'

    def setConcurrenyMode(self, concurrenyMode):
        """Lets you set the concurrency mode for a job."""
        self.concurrencyMode = concurrenyMode


    def getHeaders(self):
        return {"X-SFDC-Session": self.session.getSessionId()} # , "SForce-Line-Ending" : "CRLF"}
#                ,"SForce-Line-Ending" : "CRLF", "Sforce-Enable-PkChunking": "true"
#        }
    


    def makeHttpCall(self, requestType, endpoint, body = None, headers = None):
        self.host.request(requestType, endpoint, body, headers)
        response = self.host.getresponse()
        return (response.status, response.read())
        
           
    def createOperation(self, operation, sobject, contentType):

        body = '''<?xml version="1.0" encoding="UTF-8"?>
<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
    <operation>OPERATION</operation>
    <object>SOBJECT</object>
    <contentType>CONTENT</contentType>
</jobInfo>'''
        body = re.sub("OPERATION", operation, body)
        body = re.sub("SOBJECT", sobject, body)        
        body = re.sub("CONTENT", contentType, body)
        body = re.sub("CONCURRENCY", self.concurrencyMode, body)

        headers = self.getHeaders();
        headers["Content-Type"] = "application/xml"
        status, response = self.makeHttpCall("POST", self.endpoint + "job", body, headers)

        if  (status > 200):
            root = ET.fromstring(response)
            self.jobId = findNode(root.getchildren(), '{http://www.force.com/2009/06/asyncapi/dataload}id')
            
        return self.jobId

    def closeJobStatus(self):
        body = '''<?xml version="1.0" encoding="UTF-8"?>
<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
  <state>Closed</state>
</jobInfo>'''
        headers = self.getHeaders();
        headers["Content-Type"] = "application/xml"
        status, response = self.makeHttpCall("POST", self.endpoint + "job/" + self.jobId, body, headers)

    def getJobStatus(self, jobId=None):
        if (jobId == None):
            jobId = self.jobId
        status, response = self.makeHttpCall("GET", self.endpoint + "job/" + jobId, "", self.getHeaders())

    def postRecords(self, fileObj, contentType="text/csv", jobId = None):

        if (jobId == None):
            jobId = self.jobId

        headers = self.getHeaders();
        headers["Content-Type"] = contentType;
        status, response = self.makeHttpCall("POST", self.endpoint + "job/" + jobId + "/batch", fileObj, headers)
        if (status == 201):
            root = ET.fromstring(response)
            return findNode(root.getchildren(), '{http://www.force.com/2009/06/asyncapi/dataload}id')
        return None

    def createTransformationSpec(self, fileObj, contentType="text/csv", jobId=None):
        if (jobId == None):
            jobId = self.jobId

        headers = self.getHeaders();
        headers["Content-Type"] = contentType;
        status, response = self.makeHttpCall("POST", self.endpoint + "job/" + jobId + "/spec", fileObj, headers)
        if (status == 200):
            root = ET.fromstring(response)
            return findNode(root.getchildren(), '{http://www.force.com/2009/06/asyncapi/dataload}id')

        return None

    def queryBulk(self, sobject, query):
        self.createOperation("query", sobject, "CSV")
        batchId = self.postRecords(query)
        self.closeJobStatus()
        return batchId

