import time
from BulkApiClient import SalesforceLogin, BulkOperation
from httplib import HTTPConnection, HTTPSConnection
import math
import requests

AUTHENTICATING = "Authenticating"
CREATE_JOB = "CreateJob"
CREATE_BATCHES = "CreateBatches"
CLOSE_JOB = "CloseJob"

class BulkApiOrchestrator:
    def __init__(self, pane, client):
        self.userName='reuben@v2demo.org'
        self.password='123456'
        self.hostName = "http://rcornel-wsl2.internal.salesforce.com:6109"
        self.inputFile = "temp_batch1.txt"

        self.logger = BulkApiLogger(pane)
        self.client = client
        self.client.setLogger(self.logger)

        self.client.setUsername(self.userName);
        self.client.setPassword(self.password);
        self.client.setHostName(self.hostName);

    def runDemo(self):
        self.authenticate()
        jobId = self.createJob()
        self.uploadContent(jobId, self.inputFile)
        self.closeJob(jobId)

    def authenticate(self):
        t0 = time.time()
        self.client.authenticate()
        t1 = time.time()
        self.logger.logAuthenticationSuccess(t1 - t0)

    def createJob(self):
        (jobId, time) = self.client.createJob();
        self.logger.logJob(jobId, time)
        return jobId

    def uploadContent(self, jobId, inputFile):
        self.client.uploadContent(jobId, inputFile)
        self.logger.logBatchesComplete()

    def closeJob(self, jobId):
        t0 = time.time()
        self.client.closeJob(jobId)
        t1 = time.time()
        self.logger.logJobClose(t1 - t0);
        pass

class BulkClient:
    def setUsername(self, userName):
        self.userName = userName

    def setPassword(self, password):
        self.password = password

    def setHostName(self, hostName):
        self.isHTTPSConnection = False
        if (hostName.startswith("https:")):
            self.isHTTPSConnection = True
            self.protocol = "https://"
            self.hostName = hostName.replace("https://","");
        else:
            self.protocol = "http://"
            self.hostName = hostName.replace("http://","");

    def setLogger(self, logger):
        self.logger = logger

    def authenticate(self):
        if (self.isHTTPSConnection):
            host = HTTPSConnection(self.hostName)
        else:
            host = HTTPConnection(self.hostName)
        self.loginObj = SalesforceLogin(self.userName, self.password,  host)
        self.loginObj.login()

class BulkV2Client(BulkClient):
    BULK_V2_URL = '/services/data/v40.0/jobs/ingest'

    def authenticate(self):
        BulkClient.authenticate(self)
        self.sid = self.loginObj.getSessionId()

    def createJob(self):
        t0 = time.time()
        jobSpec = '{"contentType":"CSV", "object":"Account", "operation":"insert"}'
        resp = requests.post(self.protocol + self.hostName + self.BULK_V2_URL,
                             data=jobSpec,
                             headers={'Authorization': 'Bearer ' + self.sid, 'content-type': 'application/json'})
        jobId = resp.json()[u'id']
        self.contentUrl = resp.json()[u'contentUrl']
        t1 = time.time()
        return (jobId, t1 - t0)

    def uploadContent(self, jobId, inputFile):
        file = open(inputFile)
        t0 = time.time()
        batchData = file.read()

        # PUT the file against the url
        resp = requests.put(self.protocol + self.hostName + "/" + self.contentUrl,
                            data=batchData,
                            headers={'Authorization': 'Bearer ' + self.sid, 'content-type':'text/csv'})

        # Mark the job upload complete
        resp = requests.patch(self.protocol + self.hostName + self.BULK_V2_URL + '/' + jobId,
                       data='{"state":"UploadComplete"}',
                       headers={'Authorization': 'Bearer ' + self.sid, 'content-type':'application/json'})

        t1 = time.time()
        self.logger.logBatch("New Content", t1 - t0)

    def closeJob(self, jobId):
        pass

class BulkV1Client(BulkClient):
    def authenticate(self):
        BulkClient.authenticate(self)
        hostConnection = HTTPConnection(self.hostName)
        self.bulkOperation = BulkOperation("","", hostConnection, self.loginObj, "/services/async/38.0/")
        pass

    def createJob(self):
        t0 = time.time()
        jobId = self.bulkOperation.createOperation("insert", "Account", "CSV")
        t1 = time.time()
        return (jobId, t1 - t0)

    def uploadContent(self, jobId, inputFile):
        f = open(inputFile);
        outputFile = open("temp_batch.txt", "w")
        count = 0;
        header = f.readline()
        outputFile.write(header)
        t0 = time.time()
        t1 = t0;
        while True:
            line = f.readline()

            if(line == ""):
                break

            count = count + 1

            if (count >= 10000):
                outputFile.close()
                batchId = self.bulkOperation.postRecords(open("temp_batch.txt"), "text/csv")
                t1 = time.time();
                self.logger.logBatch(batchId, t1 - t0)
                t0 = time.time();
                outputFile = open("temp_batch.txt", "w")
                outputFile.write(header)
                count = 0;
            else:
                outputFile.write(line)

        if count > 0:
            outputFile.close()
            batchId = self.bulkOperation.postRecords(open("temp_batch.txt"), "text/csv")
            t1 = time.time();
            self.logger.logBatch(batchId, t1 - t0)

    def closeJob(self, jobId):
        self.bulkOperation.closeJobStatus()



class BulkApiLogger:
    def __init__(self, pane):
        self.pane = pane;

    def getTimeValue(self, time):
        time = round(time*1000)
        if time < 1:
            return 0
        else:
            return int(time)

    def logAuthenticationSuccess(self, time):
        time = self.getTimeValue(time)

        self.pane.insertItem(AUTHENTICATING, "AuthenticationSuccess", time)
        self.pane.markStageSuccess(AUTHENTICATING)

    def logJob(self, jobId, time):
        time = self.getTimeValue(time)

        self.pane.insertItem(CREATE_JOB, jobId, time);
        self.pane.markStageSuccess(CREATE_JOB)

    def logBatch(self, batchId, time):
        time = self.getTimeValue(time)
        self.pane.insertItem(CREATE_BATCHES, batchId, time);

    def logBatchesComplete(self):
        self.pane.markStageSuccess(CREATE_BATCHES);

    def logJobClose(self, time):
        time = self.getTimeValue(time)
        self.pane.insertItem(CLOSE_JOB, "JobCloseSuccess", time)
        self.pane.markStageSuccess(CLOSE_JOB)
