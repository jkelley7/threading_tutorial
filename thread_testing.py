import pandas as pd
import numpy as np
import re
from bs4 import BeautifulSoup
import urllib
import requests
import datetime
import time
import warnings
from fake_useragent import UserAgent
from pathlib import Path
from threading import Thread
from queue import Queue
import logging

parent_folder = Path().resolve()

logfile = parent_folder / 'logs' / 'test_log.log'
logger_fmt ='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d -%(message)s'
logging.basicConfig(level=logging.INFO,format=logger_fmt, handlers=[logging.FileHandler(logfile)])

notebook_run_date = datetime.date.today().strftime('%Y_%m_%d')
ua = UserAgent(cache=False)
warnings.filterwarnings("ignore")

browser = ['chrome', 'internetexplorer', 'firefox', 'safari', 'opera']

def get_random_header(browser):
    """choose a random header from a list of browsers"""
    return np.random.choice(browser)

def pick_random_fakeheader(ua, browsers):
    """
    Get a randomized header
    ------------------------
    Params:
    ------------------------
    ua - a useragent form fake_useragen
    browsers - a list of browsers from fake useragent

    ------------------------
    Returns:
    ------------------------
    a random choice of a header from fake useragent
    """

    browser_to_fetch = get_random_header(browsers)
    total_browsers = len(ua.data['browsers'][browser_to_fetch])
    final_choice = np.random.choice(np.arange(0,total_browsers))

    return ua.data['browsers'][browser_to_fetch][final_choice]

def create_fakeheader(ua,browsers):
    """Create the fake header
    ------------------------
    Params:
    ------------------------
    ua - a useragent form fake_useragen
    browsers- a list of browsers from fake useragent

    ------------------------
    Returns:
    ------------------------
    headers - a dictionary of a fake useragent
    """

    headers = {'User-Agent': pick_random_fakeheader(ua, browsers)}
    return headers

def crawl_no_queue(url, result_dict, index):
    """ We want to ensure we just grab everything. We can spend time parsing it later
    
    ------------------------
    Params:
    ------------------------
    url = a url
    result_dict - an empty dictionary where page elements will be stored
    index - the index number which will loop over

    ------------------------
    Returns:
    ------------------------
    return object is not possible with a thread object therefore just work on a global variable
    """
    try:
        req =  requests.get(url, verify = False, timeout = (30,30), headers = create_fakeheader(ua,browser))
        cont = req.content
        result_dict[int(index)] = cont
    except:
        result_dict[int(index)] = ''
    return True


def crawl_queue(q, result_set):
    """ We want to ensure we just grab everything. We can spend time parsing it later
    
    --------------------
    Params:
    --------------------
    q = a queue object
    result_set - an empty dictionary where page elements will be store

    --------------------
    Return:
    --------------------
    return object is not possible with a thread object therefore just work on a global variable
    """
    _log = logging.getLogger(crawl_queue.__name__)
    while not q.empty():
        worker = q.get() #get an itme from the queue

        try:
            req =  requests.get(worker[1], verify = False, timeout = (30,30), headers = create_fakeheader(ua,browser))
            cont = req.content
            result_set[worker[0]] = cont
        except:
            _log.warning(f' couldnt find a request for index {worker[0]}')
            result_set[worker[0]] = ''
        if q.qsize() % 100 == 0:
            _log.info(f'things left to process {q.qsize()}')
        q.task_done()
    return True

def parse_results(result_set):
    """
    Takes contents from requests and parses them via BeautifulSoup
    
    --------------------
    Params:
    --------------------
    result_set - a dictionary which contains content from the requests library

    --------------------
    Return:
    --------------------
    new_dict - a new dictionary with parsed elements from beautiful soup
    """
    new_dict = {}
    for key, value in result_set.items():
        page_content = BeautifulSoup(result_set[key], "html.parser")
    # find the table
        zip_table = page_content.find('table',{'class':'statTable'})
        
        zip_attributes = []
        # Loop over all the rows in the table
        for tag in zip_table.find_all('tr'):
        
        # ahh, at last, getting what we came here for, the classification
            if tag.span.text == 'Zip Code:':
                zipl = len('Zip Code:')
                zcde = tag.get_text()[zipl:]

            if tag.span.text == 'Classification:':
                cl = len('Classification:')
                zclss = tag.get_text()[cl:]
                    
            if tag.span.text == 'City Type:':
                ct = len('City Type:')
                zctytpe = tag.get_text()[ct:]
                
            if tag.span.text == 'Time Zone:':
                tz = len('Time Zone:')
                ztz = tag.get_text()[tz:]
                
            if tag.span.text == 'City:':
                ci = len('City:')
                zcty = tag.get_text()[ci:]
                
            if tag.span.text == 'State:':
                st = len('State:')
                zstte = tag.get_text()[st:]
        
        zip_attributes.extend([zcde, zclss, zctytpe, ztz, zcty, zstte])
        new_dict[key] = zip_attributes
    return new_dict


df = pd.read_csv(parent_folder / 'data' / 'raw' / 'zipcde.csv')

# duplicates in some fields
df['zip_'] = df.zip.str[:5]
df = df.drop(columns = 'zip')
df['zip_'] = df.zip_.astype(str).str.pad(width = 5, side = 'left', fillchar = '0')

# please note this is for educational uses only 
# we just want to experiment with the queue and threading option
# zip codes is a good use case for us to scrape as it's relativly straight forward
# this website where we obtained the data was relatively easy making it a good
# eductational use error checking is fairly easy

df['baselink'] = 'https://www.zip-codes.com/zip-code/'
df['finallink'] = df.baselink.map(str) + df.zip_.map(str) + '/zip-code-' + df['zip_'].map(str) + '.asp'
df['zip_class'] = ''
df['zip_city_type'] = ''
df['zip_time_zne'] = ''
df['city'] = ''
df['state'] = ''
df = df.reset_index(drop=True)

###############################################################
#
#
# THREADING
# ONLY USE THIS WHEN YOU HAVE A FEW YOU NEED TO GO THRU
# IF YOU TRY AND SCALE THIS YOU'LL GET AN ERROR
#
#
###############################################################

results = {}
threads = []
# we are only using an example of 50 as to not create too many threads.
# creating too many threads has the adverse of effect of throwing an error
for index, row in df.iloc[:50,:].iterrows():
    process = Thread(target= crawl_no_queue, args = (row['finallink'],results,index))
    process.start()
    threads.append(process)

# using join() tells one thread to wait another to finish this prevents race conditions
for process in threads:
    process.join()

for key, value in results.items():
    print(value)


return_dict = parse_results(results)
match_df = pd.DataFrame.from_dict(return_dict, orient = 'index').sort_index()
con_df = pd.concat([df.zip_[:50], match_df], axis = 1 )
con_df.columns = ['zip1', 'zip2', 'clss', 'ziptype' , 'tmz','city1', 'state']
# wahoo they both match!
con_df.query('zip1 != zip2')

###############################################################
#
#
#
# QUEUE & THREADING
#
#
#
###############################################################

# Queue experiment
nrows = df.shape[0]
thread_size = 100
q = Queue(maxsize = 0) # 0 puts all of them in the queue, if you have a number then it only puts that number in the queue
_logger = logging.getLogger('Queue Process')
for index, row in df.iterrows():
    q.put((index, row['finallink']), block = True, timeout=2)

results = {}
for i in range(thread_size):
    _logger.info(f'Starting thread {i}')
    workers = Thread(target = crawl_queue, args = (q, results))
    workers.setDaemon(True) # this ensures all threads are killed at the end of the threading
    workers.start()

# ensure all workers are done
q.join()
_logger.info('All tasks are completed')

return_dict = parse_results(results)

match_df = pd.DataFrame.from_dict(return_dict, orient = 'index').sort_index()
df_new = pd.concat([df.zip_, match_df], axis = 1 )
df_new.columns = ['old', 'new', 'clss', 'ziptype' , 'tmz','city1', 'state']
df_new.query('old != new')



