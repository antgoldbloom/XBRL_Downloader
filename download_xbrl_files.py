from time import time
import requests


import shutil
import os
import zipfile
import re

from download_sec_edgar_filings_utility_script import Downloader, create_ticker_to_cik_dict, create_cik_to_ticker_dict, write_zip, fetch_cik_list 

PATH = '../'
#PATH = '/kaggle/working/'

#options include: random_ticker_list, sp500, sample_list, debug 
LIST = 'random_ticker_list'


#initialize zip file
zip_file = zipfile.ZipFile(f"{PATH}{LIST}.zip", 'w',zipfile.ZIP_DEFLATED)

cik_list = fetch_cik_list(LIST)

count = 1

for cik in cik_list:

    start_time = time()

    cik_to_ticker_dict = create_cik_to_ticker_dict()
    print(f"{cik_to_ticker_dict[cik]} ({count} of {len(cik_list)})")

    dl = Downloader(PATH)
    dl.get("10-Q", cik)
    dl.get("10-K", cik)

    zip_file = write_zip(zip_file,PATH)
    
    try:
        shutil.rmtree(f"{PATH}sec_filings", ignore_errors=False, onerror=None)
    except:
        print("No 10-K or 10-K found for this company")

    count += 1
    print(time() - start_time)
    
zip_file.close()
