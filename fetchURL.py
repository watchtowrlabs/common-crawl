import gzip
import sys
import time
from io import BytesIO
import argparse

import boto3
import requests
import urllib3.util
import yaml

from configFile import configFile


class fetchedFile:
    def __init__(self, warcHeaders, httpHeaders, httpBody):
        self.warcHeaders = warcHeaders
        self.httpHeaders = httpHeaders
        self.httpBody = httpBody

class fetchURL:
    def __init__(self, awsAccessKeyID, awsSecretKey, awsRegion, s3bucket, crawl = 'CC-MAIN-2022-21'):
        self.s3bucket = s3bucket
        self.client = boto3.client('athena',
                                   region_name = awsRegion,
                                   aws_access_key_id=awsAccessKeyID,
                                   aws_secret_access_key=awsSecretKey)
        self.s3 = boto3.client('s3',
                                   region_name = awsRegion,
                                   aws_access_key_id=awsAccessKeyID,
                                   aws_secret_access_key=awsSecretKey)
        self.crawl = crawl

    def fetchByURL(self, url, permitTruncation = False):
        urlParsed = urllib3.util.parse_url(url)
        proto = url.split("://")[0]

        # To do this, we must query athena and obtain the WARC details for the matching file.
        queryString = f" select warc_filename, warc_record_offset, warc_record_length, content_charset, content_truncated " \
                f" FROM ccindex.ccindex " \
                f" WHERE crawl = '{self.crawl}'" \
                f" and url_protocol = '{proto}'" \
                f" and url_host_name = '{urlParsed.host}'" \
                f" and url = '{url}'"
        QueryExecutionId = self.doQuery(queryString)

        # Now we can fetch the results.
        filename = f'fetchURL_tmp/{QueryExecutionId}.csv'
        with BytesIO() as f:
            self.s3.download_fileobj(self.s3bucket, filename, f)
            f.seek(0)
            lineIdx = 0
            for line in f.readlines():
                lineIdx = lineIdx + 1
                # Skip the header line
                if lineIdx == 1:
                    continue
                warc_filename, warc_record_offset, warc_record_length, content_charset, content_truncated = line.decode("ascii").split(",")
                warc_filename = warc_filename.strip('"\n')
                warc_record_offset = int(warc_record_offset.strip('"\n'))
                warc_record_length = int(warc_record_length.strip('"\n'))

                if permitTruncation == False:
                    if len(content_truncated.strip()) > 0:
                        raise Exception("File is truncated in Common Crawl dataset")

                return self.fetchByWARCDetails(warc_filename, warc_record_offset, warc_record_length, encoding=content_charset)
            raise Exception("No file found for given URL.")

        self.s3.Object(self.s3bucket, filename)

    def fetchByWARCDetails(self, warc_filename, warc_record_offset, warc_record_length, encoding = 'ascii') -> fetchedFile:
        if encoding == "":
            encoding = "utf-8"
        with requests.request("GET", f"https://data.commoncrawl.org/{warc_filename}", headers={
            'Range': f"bytes={warc_record_offset}-{warc_record_offset + warc_record_length - 1}"},
                              stream=True) as req:
            req.raise_for_status()
            # Read compressed data from the HTTP stream into memory
            s = req.raw.stream(1024, decode_content=False)
            gz = BytesIO()
            for chunk in s:
                gz.write(chunk)
            # And decompress.
            gz.seek(0)
            payload = b''
            with gzip.GzipFile(fileobj=gz) as f:
                while True:
                    thisChunk = f.read()
                    if thisChunk is None or len(thisChunk) == 0:
                        break
                    payload = payload + thisChunk
        warc, http, payload = payload.decode(encoding).split("\r\n\r\n", 2)
        return fetchedFile(warc, http, payload)

    def doQuery(self, queryString):
        queryStart = self.client.start_query_execution(
            QueryString=queryString,
            QueryExecutionContext={
                'Database': 'ccindex'
            },
            ResultConfiguration={'OutputLocation': f's3://{self.s3bucket}/fetchURL_tmp'}
        )
        QueryExecutionId = queryStart['QueryExecutionId']

        # Wait for this query to complete
        delay = 1
        while True:
            queryExecution = self.client.get_query_execution(QueryExecutionId=QueryExecutionId)
            state = queryExecution['QueryExecution']['Status']['State']
            if state in ("QUEUED", "RUNNING"):
                if delay < 60:
                    delay = delay * 3
                time.sleep(delay)
                continue
            break
        if state != "SUCCEEDED":
            raise Exception(f"Query did not succeed, it is in state '{state}'")

        return QueryExecutionId


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Fetch files from the Common Crawl dataset')

    parser.add_argument('--warc-headers', action="store_true", default=False, help="Show only WARC medatada")
    parser.add_argument('--http-headers', action="store_true", default=False, help="Show only HTTP headers")
    parser.add_argument('--http-body', action="store_true", default=False, help="Show only HTTP response body")
    parser.add_argument('--permit-truncation', action="store_true", default=False, help="Show partial responses if data is truncated")
    parser.add_argument('--crawl', type=str, default="CC-MAIN-2022-33", help="Crawl to use (For example, 'CC-MAIN-2022-33")
    parser.add_argument('url', help="The URL to fetch, eg, https://watchtowr.com/robots.txt")

    args = parser.parse_args()

    cfgFile = configFile('config.yaml')

    fetcher = fetchURL(cfgFile.accessKey, cfgFile.secretKey, cfgFile.availabilityZone, cfgFile.bucketName, crawl=args.crawl)
    response = fetcher.fetchByURL(args.url, permitTruncation = args.permit_truncation)
    if args.warc_headers:
        print(response.warcHeaders)
    elif args.http_headers:
        print(response.httpHeaders)
    elif args.http_body:
        print(response.httpBody)

    # By default, just print HTTP body.
    if True not in (args.warc_headers, args.http_headers, args.http_body):
        print(response.httpBody)
