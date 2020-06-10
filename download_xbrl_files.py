from time import time
import requests
import csv
import shutil
import os
import zipfile
import re
from XBRL_Downloader import Downloader, create_ticker_to_cik_dict, create_cik_to_ticker_dict  


PATH = '../'
LIST = 'sample_list'

def fetch_ticker_list(list_name='sample_list'): 
    
    if list_name == 'all_companies':
        resp = requests.get('https://www.sec.gov/include/ticker.txt')
        ticker_text = resp.content.decode('utf-8')
        ticker_list = list(csv.reader(ticker_text.splitlines(), delimiter='\t'))
        # ITERATION = 1
        # INCREMENT=10
        # end = INCREMENT*ITERATION
        # start = end - INCREMENT
        
    elif list_name == 'sp500':
        ticker_list = ['MMM','ABT','ABBV','ABMD','ACN','ATVI','ADBE','AMD','AAP','AES','AFL','A','APD','AKAM','ALK','ALB','ARE','ALXN','ALGN','ALLE','AGN','ADS','LNT','ALL','GOOGL','GOOG','MO','AMZN','AMCR','AEE','AAL','AEP','AXP','AIG','AMT','AWK','AMP','ABC','AME','AMGN','APH','ADI','ANSS','ANTM','AON','AOS','APA','AIV','AAPL','AMAT','APTV','ADM','ARNC','ANET','AJG','AIZ','ATO','T','ADSK','ADP','AZO','AVB','AVY','BKR','BLL','BAC','BK','BAX','BDX','BRK.B','BBY','BIIB','BLK','BA','BKNG','BWA','BXP','BSX','BMY','AVGO','BR','BF.B','CHRW','COG','CDNS','CPB','COF','CPRI','CAH','KMX','CCL','CAT','CBOE','CBRE','CDW','CE','CNC','CNP','CTL','CERN','CF','SCHW','CHTR','CVX','CMG','CB','CHD','CI','XEC','CINF','CTAS','CSCO','C','CFG','CTXS','CLX','CME','CMS','KO','CTSH','CL','CMCSA','CMA','CAG','CXO','COP','ED','STZ','COO','CPRT','GLW','CTVA','COST','COTY','CCI','CSX','CMI','CVS','DHI','DHR','DRI','DVA','DE','DAL','XRAY','DVN','FANG','DLR','DFS','DISCA','DISCK','DISH','DG','DLTR','D','DOV','DOW','DTE','DUK','DRE','DD','DXC','ETFC','EMN','ETN','EBAY','ECL','EIX','EW','EA','EMR','ETR','EOG','EFX','EQIX','EQR','ESS','EL','EVRG','ES','RE','EXC','EXPE','EXPD','EXR','XOM','FFIV','FB','FAST','FRT','FDX','FIS','FITB','FE','FRC','FISV','FLT','FLIR','FLS','FMC','F','FTNT','FTV','FBHS','FOXA','FOX','BEN','FCX','GPS','GRMN','IT','GD','GE','GIS','GM','GPC','GILD','GL','GPN','GS','GWW','HRB','HAL','HBI','HOG','HIG','HAS','HCA','PEAK','HP','HSIC','HSY','HES','HPE','HLT','HFC','HOLX','HD','HON','HRL','HST','HPQ','HUM','HBAN','HII','IEX','IDXX','INFO','ITW','ILMN','IR','INTC','ICE','IBM','INCY','IP','IPG','IFF','INTU','ISRG','IVZ','IPGP','IQV','IRM','JKHY','J','JBHT','SJM','JNJ','JCI','JPM','JNPR','KSU','K','KEY','KEYS','KMB','KIM','KMI','KLAC','KSS','KHC','KR','LB','LHX','LH','LRCX','LW','LVS','LEG','LDOS','LEN','LLY','LNC','LIN','LYV','LKQ','LMT','L','LOW','LYB','MTB','M','MRO','MPC','MKTX','MAR','MMC','MLM','MAS','MA','MKC','MXIM','MCD','MCK','MDT','MRK','MET','MTD','MGM','MCHP','MU','MSFT','MAA','MHK','TAP','MDLZ','MNST','MCO','MS','MOS','MSI','MSCI','MYL','NDAQ','NOV','NTAP','NFLX','NWL','NEM','NWSA','NWS','NEE','NLSN','NKE','NI','NBL','JWN','NSC','NTRS','NOC','NLOK','NCLH','NRG','NUE','NVDA','NVR','ORLY','OXY','ODFL','OMC','OKE','ORCL','PCAR','PKG','PH','PAYX','PAYC','PYPL','PNR','PBCT','PEP','PKI','PRGO','PFE','PM','PSX','PNW','PXD','PNC','PPG','PPL','PFG','PG','PGR','PLD','PRU','PEG','PSA','PHM','PVH','QRVO','PWR','QCOM','DGX','RL','RJF','RTN','O','REG','REGN','RF','RSG','RMD','RHI','ROK','ROL','ROP','ROST','RCL','SPGI','CRM','SBAC','SLB','STX','SEE','SRE','NOW','SHW','SPG','SWKS','SLG','SNA','SO','LUV','SWK','SBUX','STT','STE','SYK','SIVB','SYF','SNPS','SYY','TMUS','TROW','TTWO','TPR','TGT','TEL','FTI','TFX','TXN','TXT','TMO','TIF','TJX','TSCO','TDG','TRV','TFC','TWTR','TSN','UDR','ULTA','USB','UAA','UA','UNP','UAL','UNH','UPS','URI','UTX','UHS','UNM','VFC','VLO','VAR','VTR','VRSN','VRSK','VZ','VRTX','VIAC','V','VNO','VMC','WRB','WAB','WMT','WBA','DIS','WM','WAT','WEC','WFC','WELL','WDC','WU','WRK','WY','WHR','WMB','WLTW','WYNN','XEL','XRX','XLNX','XYL','YUM','ZBRA','ZBH','ZION','ZTS']

    elif list_name == 'sample_list':
        ticker_list = ['GOOG','MSFT','AMZN','NVDA','ORCL','INTC','CRM','TSLA','FB','AAPL','NFLX','ZM','TEAM','BYND','OKTA','ZM','ZEN','ADBE','UBER','LYFT','PINS','CRWD','SPOT','SVMK']

    ticker_to_cik_dict = create_ticker_to_cik_dict()
    cik_list = []
    for ticker in ticker_list:
        cik_list.append(ticker_to_cik_dict[ticker])
    

    return cik_list


def write_zip(zip_file,zip_root_path):
    for dirname, _, filenames in os.walk(zip_root_path):
        for filename in filenames:
            zip_full_path = os.path.join(dirname, filename)
            zip_arc_name = zip_full_path[len(zip_root_path):]
            zip_file.write(zip_full_path,f"/sec_filings{zip_arc_name}")

    
    return zip_file


start_time = time()


zip_save_path = f"{PATH}{LIST}.zip"
zip_file = zipfile.ZipFile(zip_save_path, 'w',zipfile.ZIP_DEFLATED)
zip_root_path = '{PATH}sec_filings'

cik_list = fetch_ticker_list(LIST)

for cik in cik_list:
    cik_to_ticker_dict = create_cik_to_ticker_dict()
    print(cik_to_ticker_dict[cik])

    dl = Downloader(PATH)

    dl.get("10-Q", cik)
    dl.get("10-K", cik)

    zip_file = write_zip(zip_file,zip_root_path)
    
    try:
        shutil.rmtree(f"{PATH}sec_filings", ignore_errors=False, onerror=None)
    except:
        print(f"Can't remove {PATH}sec_filings because it wasn't found")
    
zip_file.close()

print(time() - start_time)