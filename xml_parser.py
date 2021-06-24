import csv
import requests
from bs4 import BeautifulSoup
import re
import tempfile
import os
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import logging

logger = logging.getLogger()

''' Configurations'''
URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
extension = ".zip"
COLS = ['FinInstrmGnlAttrbts.Id','FinInstrmGnlAttrbts.FullNm','FinInstrmGnlAttrbts.ClssfctnTp','FinInstrmGnlAttrbts.CmmdtyDerivInd','FinInstrmGnlAttrbts.NtnlCcy','Issr']

class XmlParser():

    def __init__(self):
        self.url = URL
        self.tempdir = tempfile.mkdtemp(prefix="steeleye", suffix="zip_folder")
    
    def parsedownloadedlinks(self):

        # Request to pull the content from url
        xml_data  = requests.get(self.url).content
        soup = BeautifulSoup(xml_data, "lxml")
        child = soup.find("result")

        ##Retrieve the downloaded links
        downloadlinks = child.find_all(attrs={'name':'download_link'})
        return downloadlinks

    def retrieveDownloadedLinks(self,downloadlinks):
        ##creatine a list container
        urls_to_download_zip = []
        urlsregex = re.compile('http://\w*\.\w*\.\w*\.\w.\/\w*\/\w*\.\w*')
        for downlk in downloadlinks:
            urls_to_download_zip.append(urlsregex.findall(str(downlk))[0])

        #logger.info("The urls are "urls_to_download_zip)
        return urls_to_download_zip


    def downloadZippedfilesAndExtract(self,urls_to_download_zip):
        ##Dowloading the zipped files and extract them
        for xml_files in urls_to_download_zip:
            zipFileName = os.path.join(self.tempdir, xml_files.split("/")[-1])
            #download the file contents in binary format
            r = requests.get(xml_files)
            with open(zipFileName, "wb") as zip:
                zip.write(r.content)

        ##unzip the folders
        os.chdir(self.tempdir)
        for item in os.listdir(self.tempdir):
            if item.endswith(extension):
                filename = os.path.abspath(self.tempdir+'\\'+item)
                zip_ref = zipfile.ZipFile(filename)
                zip_ref.extractall(self.tempdir)
                zip_ref.close()
                os.remove(filename)

    def xmlparsing_and_build_DataFrame(self):
        rows = []
        for xml_files in os.listdir(self.tempdir):
            parsedfile = ET.parse(xml_files).getroot()
            maxrange = len(parsedfile[1][0][0])
            for start in range(1,maxrange):
                issr = parsedfile[1][0][0][start][0][1].text
                id = parsedfile[1][0][0][start][0][0][0].text
                fullName = parsedfile[1][0][0][start][0][0][1].text
                clsfctntp = parsedfile[1][0][0][start][0][0][3].text
                ntnlccy = parsedfile[1][0][0][start][0][0][4].text
                cmmdtyDerivInd = parsedfile[1][0][0][start][0][0][5].text
                rows.append([id,fullName,clsfctntp,cmmdtyDerivInd,ntnlccy,issr])
        df = pd.DataFrame(rows, columns=COLS)
        df.to_csv(self.tempdir + "\\finaldata.csv")








if __name__ == '__main__':
    a = XmlParser()
    links = a.parsedownloadedlinks()
    downloadedLinks = a.retrieveDownloadedLinks(links)
    a.downloadZippedfilesAndExtract(downloadedLinks)
    a.xmlparsing_and_build_DataFrame()

