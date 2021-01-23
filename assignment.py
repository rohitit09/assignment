import os
import sys
import json
import wget
import glob
import boto3
import logging
import requests
import argparse
import xmltodict
import pandas as pd
import xml.etree.ElementTree as ET
from logging.handlers import TimedRotatingFileHandler

FORMATTER = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(lineno)d ::%(message)s")
LOG_FILE = "main.log"

def get_console_handler(format):
    """handler to print logs in console 
    
    Parameters
    ----------
    format=Object, Formatter object
    """

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(format)
    return console_handler

def get_file_handler(log_file,format):
    """ handler to save logs in a file

    Parameters
    ----------
    log_file=string
    format=Object, Formatter object
    """

    file_handler = TimedRotatingFileHandler(log_file, when='midnight')
    file_handler.setFormatter(format)
    return file_handler

def get_logger(logger_name):
    """creeate logger based on given info like file name, logger name and format
    
    Parameters
    ----------
    logger_name=string
    """

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG) # better to have too much log than not enough
    logger.addHandler(get_console_handler(FORMATTER))
    logger.addHandler(get_file_handler(LOG_FILE,FORMATTER))
    logger.propagate = False
    return logger


logger = get_logger('assignment')

def upload_to_s3():
    """upload all csv files present in current directory to s3 bucket"""

    client_s3 = boto3.client('s3',
     	                    aws_access_key_id=args.aws_access_key_id,
    	                    aws_secret_access_key=args.aws_secret_access_key,
    	                    region_name=args.region_name
                            )

    files = glob.glob(os.getcwd()+"/*.csv")
    for filename in files:
        i = filename.rsplit("/",1)[1]
        bucket_location = "assignments/"+i
        logger.info(bucket_location)
        try:
            response = client_s3.upload_file(filename, args.bucket, bucket_location)
            logger.info("{} uploaded to s3".format(i))
        except Exception as e:
            logger.error("upload error {}".format(str(e)))


def generate_csv(filename):
    """extracting information from xml file and generating csv
    
    Parameters
    ----------
    filename=string
    """

    logger.info("generating csv from {} ".format(filename))
    xml_data = open(filename,'r')  # Read file
    data = xmltodict.parse(xml_data.read())
    xml_data.close()

    extracted_data_list = []
    try:
        for result in data['BizData']['Pyld']['Document']['FinInstrmRptgRefDataDltaRpt']['FinInstrm']:
            get_tag_data = result.get('TermntdRcrd',None)
            if get_tag_data is None:
                get_tag_data = result.get('NewRcrd',None)
            if get_tag_data is None:
                get_tag_data = result.get('ModfdRcrd',None)
    
            extracted_data_list.append({'Id': get_tag_data['FinInstrmGnlAttrbts']['Id'],
                                        "FullNm": get_tag_data['FinInstrmGnlAttrbts']['FullNm'],
                                        "ClssfctnTp": get_tag_data['FinInstrmGnlAttrbts']['ClssfctnTp'],
                                        "CmmdtyDerivInd": get_tag_data['FinInstrmGnlAttrbts']['CmmdtyDerivInd'],
                                        "NtnlCcy": get_tag_data['FinInstrmGnlAttrbts']['NtnlCcy'],
                                        "Issr": get_tag_data['Issr']
                                        })
            
        df = pd.DataFrame(extracted_data_list) # Write in DF
        df.to_csv(filename.replace("xml","")+'csv',index=False)
        logger.info("{} csv file generated".format(filename.replace("xml","")+'csv'))
        return "csv generated"
    except Exception as e:
        logger.error(str(e))


def parse_first_xml(file_name):
    """parsing xml file to get zip file link
    
    Parameters
    ----------
    file_name=string
    """

    root = ET.parse(file_name).getroot()
    bool_value = False
    logger.info("parsing xml to get zip name from tag download_link")

    for type_tag in root.findall('result/doc'):
        for i in type_tag.findall('str'):
                if(i.attrib['name']=='download_link'):
                    down_link=i.text
                if(i.attrib['name']=='file_type' and i.text=='DLTINS'):
                    bool_value=True
        if bool_value==True:
            bool_value = False

        filename = down_link.rsplit("/",1)[1]
        logger.info("zip file link:{}".format(down_link))
        logger.info("downloading zip{}".format(filename))
        
        try:
            wget.download(down_link)
        except Exception as e:
            logger.error(str(e))
        else:
            logger.info("{} downloaded".format(filename))
            logger.info("unzipping {} ".format(filename))
            os.system("unzip "+filename)
            logger.info("{} unzip done".format(filename))
            generate_csv(filename.replace("zip","xml"))

    return "file extracted"

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket')
    parser.add_argument('--aws_access_key_id')
    parser.add_argument('--aws_secret_access_key')
    parser.add_argument('--region_name')
    args = parser.parse_args()

    try:
        wget.download('https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100',"first.xml")
    except Exception as e:
        logger.error(str(e))
    else:
        logger.info("downloaded xml from given link")
        parse_first_xml("first.xml")

        if args.bucket is None and args.aws_access_key_id is None and args.aws_secret_access_key is None:
            logger.info("s3 bucket credentials were not provided")
            logger.info("extracted file present in cureent directory not uploaded to s3") 
        else:
            upload_to_s3()

    logger.info("done !!!")

# python3 assignment.py --aws_access_key_id  --aws_secret_access_key  --region_name  --bucket 