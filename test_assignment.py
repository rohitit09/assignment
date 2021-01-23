import os
import unittest
import assignment
import wget


class TestAssignment(unittest.TestCase):
    
    def setUp(self):
        os.system("mv *.xml ./output_csv/")
        os.system("mv *.csv ./output_csv/")
        os.system("mv *.zip ./output_csv/")

    def test_parse_first_xml(self):
        filename='''<?xml version="1.0" encoding="UTF-8"?>
                    <response>
                    <lst name="responseHeader">
                    <int name="status">0</int>
                    <int name="QTime">0</int>
                    <lst name="params">
                        <str name="q">*</str>
                        <str name="indent">true</str>
                        <str name="start">0</str>
                        <str name="fq">publication_date:[2021-01-17T00:00:00Z TO 2021-01-19T23:59:59Z]</str>
                        <str name="rows">100</str>
                        <str name="wt">xml</str>
                    </lst>
                    </lst>
                    <result name="response" numFound="4" start="0">
                    <doc>
                        <str name="checksum">852b2dde71cf114289ad95ada2a4e406</str>
                        <str name="download_link">http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip</str>
                        <date name="publication_date">2021-01-17T00:00:00Z</date>
                        <str name="_root_">46015</str>
                        <str name="id">46015</str>
                        <str name="published_instrument_file_id">46015</str>
                        <str name="file_name">DLTINS_20210117_01of01.zip</str>
                        <str name="file_type">DLTINS</str>
                        <long name="_version_">1689652084492730373</long>
                        <date name="timestamp">2021-01-23T04:56:36.643Z</date></doc>
                    </result>
                    </response>
                '''
        with open("test.xml", "w") as fp:
            fp.write(filename)
        rest=assignment.parse_first_xml("test.xml")
        self.assertEqual(rest,"file extracted")

    def test_parse_generate_csv(self):
        wget.download('http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip')
        os.system('unzip DLTINS_20210117_01of01.zip')
        rest=assignment.generate_csv('DLTINS_20210117_01of01.xml')
        self.assertEqual(rest,"csv generated")

    def tearDown(self):
        os.system("rm -rf *.xml")
        os.system("rm -rf *.zip")
        os.system("rm -rf *.csv")


