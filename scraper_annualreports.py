from bs4 import BeautifulSoup as soup
import requests as r
import pandas
import time
import os
import datetime
import random
import shutil
from string import punctuation

def create_input_file(input_path, raw_filename, raw_sheetname):
    """ Create pandas dataframe out of Excel file for scraping below
    Args:
        input path
        file name of the Excel file in the directory of consideration
        sheet name of the Excel file
    Returns:
        pandas dataframe 
    """
    fpath = input_path + raw_filename
    return pandas.read_excel(fpath, sheet_name  = raw_sheetname)
    
    
def set_firm_list(file, firms):
    """ Creates a list of firms for PDF download
    Args:
        Either several firms (comma-seperated list) or a '-' for all S&P500.
    Returns:
        A list of firms for parsing.
    """
    # 1. Check correctness of zip code inputs
    for i in range(0,len(firms)):
        if len(firms[0]) == 1 and len(firms) == 1 and firms[0] == "-":
            return file['Symbol'].values.tolist()
        elif len(firms[0]) != 1:
            return firms
        
def reveal_true_firm_name(file, firm):
    """Reveals true frim name upon enterig code
    Args:
        Firm code
        Input file with mapping
    Returns:
        Company name
    """
    try:
        return file[file.Symbol == firm].Company.values.tolist()[0]
    except:
        return 'unknown'

def select_first_letter(firm, file):
    """ Select first letter from firm in loop to construct listings URL.
    Args:
        firm abbreviation
        input file with firm name and firm code
    Returns:
        First letter of firm in lower case
    """
    try:
        return file[file.Symbol == firm].Company.values.tolist()[0][0].lower()
    except (KeyError, IndexError):
        return 'a'
    
def set_url(first_letter, stock_exchange, firm ,year):
    """ Constructs URL for extracting PDF document
    Args:
        First letter of firm 
        Name of stock exchange (e.g. NYSE or NASDAQ)
        Firm code
        Year of annual report
    Returns:
        Callable URL for http get request
    """
    return "http://annualreports.com/HostedData/AnnualReportArchive/" + first_letter +"/" + stock_exchange + "_"+ firm \
    + "_" + str(year) + ".pdf"

def adjust_firm_list(firm, firms):
    """ Adjusts firms list to restart properly after crash
    Args:
        Current firm (string)
        All firms (list)
    Returns:
        Jumps to page where last stopped
    """
    return firms[firms.index(firm):len(firms)]

def clean_firm_name(true_firm_name):
    """ Cleans special characters from firm name to facilitate creation
    of directory.
    Ars:
        True firm name
    Returns:
        Cleaned firm name
    """
    symbols = ['!', '"', '#', '$', '%', "'", '(', ')', '*', '+', ',', '-',
               '.', '/', ':', ';', '<', '=', '>', '?', '@', '[', '\\', ']', '^', '`', '{',
               '|', '}', '~']
    for symbol in symbols:
        true_firm_name = true_firm_name.replace(symbol, '_')
    return true_firm_name

def scrape_annualreports(file, firms, stock_exchanges, start_year, end_year, input_path,
                         raw_filename, raw_sheetname, output_path, now_str, max_repeats):
    """ Scrape annual reports from annualreports.com
    Args:
        input file with firm name and firm code
        firm abbreviation
        Name of stock exchange (e.g. NYSE or NASDAQ)
    Returns:
        Saves PDF documents in indicated folder, creating folder with scraping 
        time firm code and year
    """
    on_repeat = False
    first_run = True
    counter = 0
    while on_repeat or first_run:
        counter += 1
        if counter >= max_repeats:
            break
        print("Running iteration", counter, "of parser ...")
        try:
            # Set firms list 
            firms = set_firm_list(file, firms)
            for firm in firms:
                # Reveal true name
                true_firm = clean_firm_name(reveal_true_firm_name(file, firm))
                # Shorten firm list in case of crash
                firms = adjust_firm_list(firm, firms)
                print("Parsing annual reports of",true_firm,"...")
                # Create subfolder for firm of consideration
                firm_folder_name = output_path + now_str + "\\" + true_firm + "\\" 
                os.mkdir(firm_folder_name)
                for stock_exchange in stock_exchanges:
                    for year in range(start_year, end_year+1):
                        # Timeout
                        time.sleep(random.randint(1,3))
                        # Define URL
                        url = set_url(select_first_letter(firm, file), stock_exchange, firm ,year) 
                        # Get response code
                        response = r.get(url, timeout = 60)
                        # Extract file dpending on the response
                        if response.status_code == 200:
                            outfile = firm_folder_name  + firm + "_" + str(year) + ".pdf"
                            with open(outfile, 'wb') as f:
                                first_run = False
                                on_repeat = False
                                f.write(response.content)
                        else:
                            continue
        except r.exceptions.ConnectionError:
            print("Connection was interrupted, waiting a few moments before continuing...")
            time.sleep(random.randint(2,5) + counter)
            on_repeat = True
            # Delete folder and restart
            shutil.rmtree(firm_folder_name, ignore_errors=True)
            continue
        except TypeError:
            print("Error encountered, skipping firm ...")
            continue

def main():
    # Capture start and end time for performance
    start_time = time.time() 
    
    # Create folder for current scrape
    # Set now string
    now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Set output path
    input_path = "C:\\Users\\Calogero\\Documents\\GitHub\\scraper_annual_reports\\data\\"
    output_path = "C:\\Users\\Calogero\\Documents\\GitHub\\scraper_annual_reports\\data\\"
   
    # Set maximum repeats before crash
    max_repeats = 30
    
    # Create folder for listing output files
    time_folder = output_path + now_str
    os.mkdir(time_folder)

    # Import file with firm level codes
    raw_filename = "Sampling_S&P_500.xlsx"
    raw_sheetname = "S&P500_sampling"
    file = create_input_file(input_path, raw_filename, raw_sheetname)
    
    # Set start and end year of consideration
    start_year = 1995
    
    end_year = 2017
    
    # Set firms to be scraped (either '-' for all S&P500 or Firm code from list)
    firms = ['-']
   
    # Set names of stock exchanges to be read
    stock_exchanges = ['NYSE', 'NASDAQ']
    
    # Run scraper
    scrape_annualreports(file, firms, stock_exchanges, start_year, end_year, input_path, raw_filename, raw_sheetname, output_path, now_str, max_repeats)
    
    end_time = time.time()
    duration = time.strftime("%H:%M:%S", time.gmtime(end_time - start_time))

    final_text = "Your query was successful! Time elapsed:" + str(duration)
    print(final_text)
    time.sleep(0.5) 


# Execute scraping    
if __name__ == "__main__":
    main()
