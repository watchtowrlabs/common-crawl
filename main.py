import gzip
import time
from io import BytesIO

import boto3 as boto3
import requests as requests

class wordlist:
    def __init__(self):
        self.words = {}

    def addDocument(self, document):
        # We'll split on any of these characters
        for delim in " \r@{}[]()<>,.='\"&;:/\\%":
            document = document.replace(delim, "\n")

        for word in document.split():
            self.words[word] = self.words.get(word, 0) + 1

    def printAll(self):
        sortedWords = sorted(self.words.keys(), key=lambda x: self.words[x])
        for word in sortedWords:
            print(f"Seen {self.words[word]} time(s): '{word}'")

class wordlistFinder:
    def __init__(self, awsAccessKeyID, awsSecretKey, awsRegion, s3bucket, targetDomainName):
        self.s3bucket = s3bucket
        self.domainName = targetDomainName
        self.client = boto3.client('athena',
                                   region_name = awsRegion,
                                   aws_access_key_id=awsAccessKeyID,
                                   aws_secret_access_key=awsSecretKey)
        self.s3 = boto3.client('s3',
                                   region_name = awsRegion,
                                   aws_access_key_id=awsAccessKeyID,
                                   aws_secret_access_key=awsSecretKey)
        self.crawl = 'CC-MAIN-2022-21'

    def close(self):
        self.client.close()

    def makeWordList(self):

        queryStart = self.client.start_query_execution(
            QueryString=
                f" select warc_filename, warc_record_offset, warc_record_length "
                f" FROM ccindex.ccindex "
                f" WHERE crawl = '{self.crawl}'"
                f" and url_host_registered_domain = '{self.domainName}'",
                QueryExecutionContext={
                    'Database': 'ccindex'
                },
            ResultConfiguration={'OutputLocation': f's3://{self.s3bucket}/wordlist'}
        )
        QueryExecutionId = queryStart['QueryExecutionId']

        delay = 1
        while True:
            queryExecution = self.client.get_query_execution(QueryExecutionId = QueryExecutionId)
            state = queryExecution['QueryExecution']['Status']['State']
            if state in ("QUEUED", "RUNNING"):
                if delay < 60:
                    delay = delay * 3
                time.sleep(delay)
                continue
            break
        if state != "SUCCEEDED":
            raise Exception(f"Query did not succeed, it is in state '{state}'")

        words = wordlist()

        with BytesIO() as f:
            self.s3.download_fileobj(self.s3bucket, f'wordlist/{QueryExecutionId}.csv', f)
            f.seek(0)
            lineIdx = 0
            for line in f.readlines():
                lineIdx = lineIdx + 1
                if lineIdx == 1:
                    continue
                warc_filename, warc_record_offset, warc_record_length = line.decode("ascii").split(",")
                warc_filename = warc_filename.strip('"\n')
                warc_record_offset = int(warc_record_offset.strip('"\n'))
                warc_record_length = int(warc_record_length.strip('"\n'))

                with requests.request("GET", f"https://data.commoncrawl.org/{warc_filename}", headers={'Range': f"bytes={warc_record_offset}-{warc_record_offset+warc_record_length-1}"}, stream=True) as req:
                    req.raise_for_status()
                    # Read compressed data from the HTTP stream into memory
                    s = req.raw.stream(1024, decode_content=False)
                    gz = BytesIO()
                    for chunk in s:
                        gz.write(chunk)
                    # And decompress the gzip'ped data.
                    gz.seek(0)
                    with gzip.GzipFile(fileobj=gz) as ungz:
                        while True:
                            chunk = ungz.read()
                            if chunk is None or chunk == b'':
                                break
                            words.addDocument(chunk.decode("ascii", errors='ignore') )
        words.printAll()

if __name__ == "__main__":
    f = wordlistFinder("<access ID here>", "<secret key here>", "ap-southeast-1", "<s3 bucket name here>", "<name of target site>")
    f.makeWordList()