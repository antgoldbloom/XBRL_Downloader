# %% [markdown]
# Credit to this Github repo: which I cloned and modified for my purpose
# https://github.com/jadchaar/sec-edgar-downloader
#     

# %% [code]
"""Constants used throughout the package."""

SEC_EDGAR_BASE_URL = "https://www.sec.gov/cgi-bin/browse-edgar?"
SUPPORTED_FILINGS = {
    "4",
    "8-K",
    "10-K",
    "10KSB",
    "10-Q",
    "13F-NT",
    "13F-HR",
    "20-F",
    "SC 13G",
    "SD",
    "S-1",
    "DEF 14A",
}
W3_NAMESPACE = {"w3": "http://www.w3.org/2005/Atom"}


"""Utility functions for the downloader class."""

import re
import time
from collections import namedtuple
from datetime import datetime
from urllib.parse import urlencode

import requests
from lxml import etree
from bs4 import BeautifulSoup
import csv
import np

import random
import os
#from _constants import SEC_EDGAR_BASE_URL, W3_NAMESPACE

FilingMetadata = namedtuple("FilingMetadata", ["ticker", "url_base","xbrl_files","period_end"])
                

def random_ticker_list(list_size=500):
    resp = requests.get('https://www.sec.gov/include/ticker.txt')
    ticker_cik_text = resp.content.decode('utf-8')
    ticker_cik_list = list(csv.reader(ticker_cik_text.splitlines(), delimiter='\t'))
    
    ticker_list = []
    for item in ticker_cik_list:
        ticker_list.append(item[0].upper())

    return random.sample(ticker_list, list_size)

def fetch_cik_list(list_name='sample_list'): 
    
    if list_name == 'random_ticker_list':
       ticker_list = random_ticker_list() 
    elif list_name == 'sp500':
        ticker_list = ['MMM','ABT','ABBV','ABMD','ACN','ATVI','ADBE','AMD','AAP','AES','AFL','A','APD','AKAM','ALK','ALB','ARE','ALXN','ALGN','ALLE','AGN','ADS','LNT','ALL','GOOG','MO','AMZN','AMCR','AEE','AAL','AEP','AXP','AIG','AMT','AWK','AMP','ABC','AME','AMGN','APH','ADI','ANSS','ANTM','AON','AOS','APA','AIV','AAPL','AMAT','APTV','ADM','ARNC','ANET','AJG','AIZ','ATO','T','ADSK','ADP','AZO','AVB','AVY','BKR','BLL','BAC','BK','BAX','BDX','BBY','BIIB','BLK','BA','BKNG','BWA','BXP','BSX','BMY','AVGO','BR','CHRW','COG','CDNS','CPB','COF','CPRI','CAH','KMX','CCL','CAT','CBOE','CBRE','CDW','CE','CNC','CNP','CTL','CERN','CF','SCHW','CHTR','CVX','CMG','CB','CHD','CI','XEC','CINF','CTAS','CSCO','C','CFG','CTXS','CLX','CME','CMS','KO','CTSH','CL','CMCSA','CMA','CAG','CXO','COP','ED','STZ','COO','CPRT','GLW','CTVA','COST','COTY','CCI','CSX','CMI','CVS','DHI','DHR','DRI','DVA','DE','DAL','XRAY','DVN','FANG','DLR','DFS','DISCA','DISCK','DISH','DG','DLTR','D','DOV','DOW','DTE','DUK','DRE','DD','DXC','ETFC','EMN','ETN','EBAY','ECL','EIX','EW','EA','EMR','ETR','EOG','EFX','EQIX','EQR','ESS','EL','EVRG','ES','RE','EXC','EXPE','EXPD','EXR','XOM','FFIV','FB','FAST','FRT','FDX','FIS','FITB','FE','FISV','FLT','FLIR','FLS','FMC','F','FTNT','FTV','FBHS','FOXA','FOX','BEN','FCX','GPS','GRMN','IT','GD','GE','GIS','GM','GPC','GILD','GL','GPN','GS','GWW','HRB','HAL','HBI','HOG','HIG','HAS','HCA','PEAK','HP','HSIC','HSY','HES','HPE','HLT','HFC','HOLX','HD','HON','HRL','HST','HPQ','HUM','HBAN','HII','IEX','IDXX','INFO','ITW','ILMN','IR','INTC','ICE','IBM','INCY','IP','IPG','IFF','INTU','ISRG','IVZ','IPGP','IQV','IRM','JKHY','J','JBHT','SJM','JNJ','JCI','JPM','JNPR','KSU','K','KEY','KEYS','KMB','KIM','KMI','KLAC','KSS','KHC','KR','LB','LHX','LH','LRCX','LW','LVS','LEG','LDOS','LEN','LLY','LNC','LIN','LYV','LKQ','LMT','L','LOW','LYB','MTB','M','MRO','MPC','MKTX','MAR','MMC','MLM','MAS','MA','MKC','MXIM','MCD','MCK','MDT','MRK','MET','MTD','MGM','MCHP','MU','MSFT','MAA','MHK','TAP','MDLZ','MNST','MCO','MS','MOS','MSI','MSCI','MYL','NDAQ','NOV','NTAP','NFLX','NWL','NEM','NWSA','NWS','NEE','NLSN','NKE','NI','NBL','JWN','NSC','NTRS','NOC','NLOK','NCLH','NRG','NUE','NVDA','NVR','ORLY','OXY','ODFL','OMC','OKE','ORCL','PCAR','PKG','PH','PAYX','PAYC','PYPL','PNR','PBCT','PEP','PKI','PRGO','PFE','PM','PSX','PNW','PXD','PNC','PPG','PPL','PFG','PG','PGR','PLD','PRU','PEG','PSA','PHM','PVH','QRVO','PWR','QCOM','DGX','RL','RJF','RTN','O','REG','REGN','RF','RSG','RMD','RHI','ROK','ROL','ROP','ROST','RCL','SPGI','CRM','SBAC','SLB','STX','SEE','SRE','NOW','SHW','SPG','SWKS','SLG','SNA','SO','LUV','SWK','SBUX','STT','STE','SYK','SIVB','SYF','SNPS','SYY','TMUS','TROW','TTWO','TPR','TGT','TEL','FTI','TFX','TXN','TXT','TMO','TIF','TJX','TSCO','TDG','TRV','TFC','TWTR','TSN','UDR','ULTA','USB','UAA','UA','UNP','UAL','UNH','UPS','URI','UTX','UHS','UNM','VFC','VLO','VAR','VTR','VRSN','VRSK','VZ','VRTX','V','VNO','VMC','WRB','WAB','WMT','WBA','DIS','WM','WAT','WEC','WFC','WELL','WDC','WU','WRK','WY','WHR','WMB','WLTW','WYNN','XEL','XRX','XLNX','XYL','YUM','ZBRA','ZBH','ZION','ZTS']

    elif list_name == 'sample_list':
        ticker_list = ['GOOG','MSFT','AMZN','NVDA','ORCL','INTC','CRM','TSLA','FB','AAPL','NFLX','ZM','BYND','OKTA','ZEN','ADBE','UBER','LYFT','PINS','CRWD','SVMK']
    elif list_name == 'debug':
        ticker_list = ['ZM']

    ticker_to_cik_dict = create_ticker_to_cik_dict()
    cik_list = []
    for ticker in np.unique(ticker_list): #add np.unique to make sure no duplicate tickers are in the ticker lists
        cik_list.append(ticker_to_cik_dict[ticker])
    

    return cik_list


def write_zip(zip_file,PATH):
    zip_root_path = f"{PATH}sec_filings"
    for dirname, _, filenames in os.walk(zip_root_path):
        for filename in filenames:
            zip_full_path = os.path.join(dirname, filename)
            zip_arc_name = zip_full_path[len(zip_root_path):]
            zip_file.write(zip_full_path,f"/sec_filings{zip_arc_name}")

    
    return zip_file


def download_ticker_cik_list():
    resp = requests.get('https://www.sec.gov/include/ticker.txt')
    ticker_text = resp.content.decode('utf-8')
    return list(csv.reader(ticker_text.splitlines(), delimiter='\t'))
    
def create_ticker_to_cik_dict():
    ticker_to_cik = dict()
    ticker_cik_list = download_ticker_cik_list()
    
    for item in ticker_cik_list:
        ticker_to_cik[item[0].upper()] = item[1]
    
    return ticker_to_cik
   
def create_cik_to_ticker_dict():
    cik_to_ticker = dict()
    ticker_cik_list = download_ticker_cik_list()
    
    for item in ticker_cik_list:
        cik_to_ticker[item[1]] = item[0].upper()
    
    return cik_to_ticker


def validate_date_format(date_str):
    try:
        datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        raise ValueError(
            "Incorrect date format. Please enter a date string of the form YYYYMMDD."
        )


def form_query_string(
    start, count, cik, filing_type, before_date, ownership="exclude"
):
    query_params = {
        "action": "getcompany",
        "owner": ownership,
        "start": start,
        "count": count,
        "CIK": cik,
        "type": filing_type,
        "dateb": before_date,
        "output": "atom",
    }
    return urlencode(query_params)


def extract_elements_from_xml(xml_byte_object, xpath_selector):
    xml_root = etree.fromstring(xml_byte_object)
    return xml_root.xpath(xpath_selector, namespaces=W3_NAMESPACE)


def get_filing_urls_to_download(
    download_folder,
    filing_type,
    cik,
    num_filings_to_download,
    after_date,
    before_date,
    include_amends,
):
    filings_to_fetch = []
    start = 0
    count = 100
    url_without_xbrl = []

    #with open(f"{download_folder}not_xbrl.log", "w", encoding="utf-8") as f:
    #    f.write("URLs without XBRL\n")

    # loop until:
    # (1) we get more filings than num_filings_to_download
    # (2) there are no more filings to fetch
    while len(filings_to_fetch) < num_filings_to_download:
        # form 4 requires ownership to be set to "only" or
        # else we will only fetch form 424B5
        if filing_type == "4":
            ownership = "only"
        else:
            ownership = "exclude"

        qs = form_query_string(
            start, count, cik, filing_type, before_date, ownership
        )
        edgar_search_url = f"{SEC_EDGAR_BASE_URL}{qs}"

        try:
            resp = requests.get(edgar_search_url)
            resp.raise_for_status()
        except:
            print("failed to download {edgar_search_url}")
            return []

        # An HTML page is returned when an invalid ticker is entered
        # Filter out non-XML responses
        if resp.headers["Content-Type"] != "application/atom+xml":
            return []

        # Need xpath capabilities of lxml because some entries are mislabeled as
        # 10-K405, for example, which makes an exact match of filing type infeasible
        if include_amends:
            xpath_selector = "//w3:content"
        else:
            xpath_selector = "//w3:filing-type[not(contains(text(), '/A'))]/.."

        filing_entry_elts = extract_elements_from_xml(resp.content, xpath_selector)

        # no more filings available
        if not filing_entry_elts:
            break

        for elt in filing_entry_elts:
            # after date constraint needs to be checked if it is passed in
            if after_date is not None:
                filing_date = elt.findtext("w3:filing-date", namespaces=W3_NAMESPACE)
                filing_date = filing_date.replace("-", "", 2)
                if filing_date < after_date:
                    return filings_to_fetch[:num_filings_to_download]

            search_result_url = elt.findtext("w3:filing-href", namespaces=W3_NAMESPACE)

            url_base = '/'.join(search_result_url.split("/")[:-1])

            resp = requests.get(search_result_url)
            soup = BeautifulSoup(resp.content, 'lxml') 
            
            try:
                xbrl_files = dict()
                xbrl_files['htm']=soup.find(text=re.compile('XBRL INSTANCE DOCUMENT|EX-101.INS')).parent.parent.find('a',href=re.compile('xml')).get_text()
                xbrl_files['lab']=soup.find('td',text='EX-101.LAB').parent.find('a',href=re.compile('xml')).get_text()
                xbrl_files['pre']=soup.find('td',text='EX-101.PRE').parent.find('a',href=re.compile('xml')).get_text()

                period_end=soup.find('div',text='Period of Report').parent.find('div',text=re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}')).get_text()
            
                filings_to_fetch.append(
                    FilingMetadata(ticker=cik, url_base=url_base,xbrl_files=xbrl_files,period_end=period_end)
                )
            except:
                match_url_format = re.match("https://www.sec.gov/Archives/edgar/data/[0-9]+/[0-9]+/[0-9]+-[0-9]{2}",search_result_url)           
                if match_url_format: #these could be an exception but they're all very old so not bothering
                    failed_year = int(match_url_format[0][-2:])
                    if (failed_year > 11) and (failed_year < 25): 
                        url_without_xbrl.append(search_result_url)
                

        start += count

        if len(url_without_xbrl) > 0:
            print(f"{filing_type} URLs with no XBRL " + ' '.join(url_without_xbrl))
        
    return filings_to_fetch[:num_filings_to_download]


def download_filings(download_folder, cik, filing_type, filings_to_fetch):
    for filing in filings_to_fetch:

        for x_file in filing.xbrl_files:
            try:
            
                resp = requests.get(f"{filing.url_base}/{filing.xbrl_files[x_file]}")
                resp.raise_for_status()

                cik_to_ticker_dict = create_cik_to_ticker_dict()
                ticker = cik_to_ticker_dict[cik]
                
                save_path = download_folder.joinpath(
                    "sec_filings", ticker, filing.period_end,filing.xbrl_files[x_file] 
                )

            # Create all parent directories as needed
                save_path.parent.mkdir(parents=True, exist_ok=True)

                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(resp.text)

            # SEC limits users to 10 downloads per second
            # Sleep >0.10s between each download to prevent rate-limiting
            # https://github.com/jadchaar/sec-edgar-downloader/issues/24
                time.sleep(0.15)
            except:
                print(f"Failed to download {filing.url_base}/{filing.xbrl_files[x_file]}")
                
                
                
"""Provides the :class:`Downloader` class, which is used to download SEC filings."""

import sys
from datetime import date
from pathlib import Path

#from _constants import SUPPORTED_FILINGS
#from _utils import download_filings, get_filing_urls_to_download, validate_date_format


class Downloader:
    """
    A :class:`Downloader` object.

    :param download_folder: relative or absolute path to download location,
        defaults to the user's ``Downloads`` folder.
    :type download_folder: ``str``, optional

    Usage::

        >>> from sec_edgar_downloader import Downloader
        >>> dl = Downloader()
    """

    def __init__(self, download_folder=None):
        """Constructor for the :class:`Downloader` class."""
        if download_folder is None:
            self.download_folder = Path.home().joinpath("Downloads")
        else:
            self.download_folder = Path(download_folder).expanduser().resolve()

    @property
    def supported_filings(self):
        """Get a sorted list of all supported filings.

        :return: sorted list of all supported filings.
        :rtype: ``list``

        Usage::

            >>> from sec_edgar_downloader import Downloader
            >>> dl = Downloader()
            >>> dl.supported_filings
            ['10-K', '10-Q', '10KSB', '13F-HR', '13F-NT', '8-K', 'SC 13G', 'SD']
        """
        return sorted(SUPPORTED_FILINGS)

    def get(
        self,
        filing_type,
        cik,
        num_filings_to_download=None,
        after_date=None,
        before_date=None,
        include_amends=False,
    ):
        """Downloads filing documents and saves them to disk.

        :param filing_type: type of filing to download
        :type filing_type: ``str``
        :param ticker_or_cik: ticker or CIK to download filings for
        :type ticker_or_cik: ``str``
        :param num_filings_to_download: number of filings to download,
            defaults to all available filings
        :type num_filings_to_download: ``int``, optional
        :param after_date: date of form YYYYMMDD in which to download filings after,
            defaults to None
        :type after_date: ``str``, optional
        :param before_date: date of form YYYYMMDD in which to download filings before,
            defaults to today
        :type before_date: ``str``, optional
        :param include_amends: denotes whether or not to include filing amends (e.g. 8-K/A),
            defaults to False
        :type include_amends: ``bool``, optional
        :return: number of filings downloaded
        :rtype: ``int``

        Usage::

            >>> from sec_edgar_downloader import Downloader
            >>> dl = Downloader()

            # Get all 8-K filings for Apple
            >>> dl.get("8-K", "AAPL")

            # Get all 8-K filings for Apple, including filing amends (8-K/A)
            >>> dl.get("8-K", "AAPL", include_amends=True)

            # Get all 8-K filings for Apple after January 1, 2017 and before March 25, 2017
            >>> dl.get("8-K", "AAPL", after_date="20170101", before_date="20170325")

            # Get the five most recent 10-K filings for Apple
            >>> dl.get("10-K", "AAPL", 5)

            # Get all 10-Q filings for Visa
            >>> dl.get("10-Q", "V")

            # Get all 13F-NT filings for the Vanguard Group
            >>> dl.get("13F-NT", "0000102909")

            # Get all 13F-HR filings for the Vanguard Group
            >>> dl.get("13F-HR", "0000102909")

            # Get all SC 13G filings for Apple
            >>> dl.get("SC 13G", "AAPL")

            # Get all SD filings for Apple
            >>> dl.get("SD", "AAPL")
        """
        if filing_type not in SUPPORTED_FILINGS:
            filing_options = ", ".join(sorted(SUPPORTED_FILINGS))
            raise ValueError(
                f"'{filing_type}' filings are not supported. "
                f"Please choose from the following: {filing_options}."
            )

        cik = str(cik).strip().lstrip("0")

        if num_filings_to_download is None:
            # obtain all available filings, so we simply
            # need a large number to denote this
            num_filings_to_download = sys.maxsize
        else:
            num_filings_to_download = int(num_filings_to_download)
            if num_filings_to_download < 1:
                raise ValueError(
                    "Please enter a number greater than 1 "
                    "for the number of filings to download."
                )

        # no sensible default exists for after_date
        if after_date is not None:
            after_date = str(after_date)
            validate_date_format(after_date)

        if before_date is None:
            before_date = date.today().strftime("%Y%m%d")
        else:
            before_date = str(before_date)
            validate_date_format(before_date)

        if after_date is not None and after_date > before_date:
            raise ValueError(
                "Invalid after_date and before_date. "
                "Please enter an after_date that is less than the before_date."
            )

        filings_to_fetch = get_filing_urls_to_download(
            self.download_folder,
            filing_type,
            cik,
            num_filings_to_download,
            after_date,
            before_date,
            include_amends,
        )

        download_filings(
            self.download_folder, cik, filing_type, filings_to_fetch
        )

        return len(filings_to_fetch)


# %% [code]
