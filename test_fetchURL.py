from unittest import TestCase

from fetchURL import configFile, fetchURL


class TestfetchURL(TestCase):
    def test_open_by_warcdetails(self):
        cfgFile = configFile('config.yaml')

        uut = fetchURL(cfgFile.accessKey, cfgFile.secretKey, cfgFile.availabilityZone, cfgFile.bucketName)
        with uut.fetchByWARCDetails(
            'crawl-data/CC-MAIN-2022-33/segments/1659882571989.67/robotstxt/CC-MAIN-20220813232744-20220814022744-00788.warc.gz',
            1407382,
            723) as f:
            data = f.read()
            # We should see the WARC header first
            self.assertTrue(data.startswith(b"WARC/1.0"))
            # We should see the HTTP headers start after two consecutive newlines
            httpData = data.split(b"\r\n\r\n")[1]
            print(httpData)
            self.assertTrue(httpData.startswith(b"HTTP/1.1 200 OK"))
            # And the data payload should follow this.
            httpPayload = data.split(b"\r\n\r\n")[2]
            print(httpPayload)
            self.assertEqual(len(httpPayload), 25)
            self.assertEqual(b"User-agent: *\nDisallow: /", httpPayload)

    def test_open_by_URL(self):
        cfgFile = configFile('config.yaml')

        uut = fetchURL(cfgFile.accessKey, cfgFile.secretKey, cfgFile.availabilityZone, cfgFile.bucketName, crawl='CC-MAIN-2022-33')
        with uut.fetchByURL('https://watchtowr-2.webflow.io/robots.txt') as f:
            data = f.read()
            # We should see the WARC header first
            self.assertTrue(data.startswith(b"WARC/1.0"))
            # We should see the HTTP headers start after two consecutive newlines
            httpData = data.split(b"\r\n\r\n")[1]
            print(httpData)
            self.assertTrue(httpData.startswith(b"HTTP/1.1 200 OK"))
            # And the data payload should follow this.
            httpPayload = data.split(b"\r\n\r\n")[2]
            print(httpPayload)
            self.assertEqual(len(httpPayload), 25)
            self.assertEqual(b"User-agent: *\nDisallow: /", httpPayload)
