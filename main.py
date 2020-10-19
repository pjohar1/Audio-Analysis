# -*- coding: utf-8 -*-
"""
Created on Wed Oct 14 17:26:25 2020

@author: punar
"""

import os
#import numpy as np
import pandas as pd 
import worker

from multiprocessing import Pool

import time
#from datetime import datetime, timedelta
#from random import seed


# Auxilliary Functions to record execution time

def secondsToTime(seconds):
    min, sec = divmod(seconds, 60) 
    hour, min = divmod(min, 60) 
    return "%d:%02d:%02d" % (hour, min, sec) + " h:m:s"

def printElapsedTime(startTime):
    endTime = time.time()
    totalTime = endTime - startTime
    print("Elapsed time:",secondsToTime(totalTime))
    
def process_files_parallel(path_ticker):
    ''' Process each file in parallel via Poll.map() '''
    pool=Pool(processes=3)
    results=pool.map(worker.process_file, os.listdir(path_ticker))
    return results



if __name__ ==  '__main__':
    
    dir_timestamp = '/data/project/audio_tone/all_files/Timestamps Tickers/'
    dir_max_time = '/data/project/audio_tone/all_files/Max Speaker Time Ticker/'
    
    startTime = time.time()

    for ticker in os.listdir(dir_timestamp):
        path = os.path.join(dir_max_time, ticker)
        if not os.path.isdir(path):
            os.mkdir(path)

        process_files_parallel(os.path.join(dir_timestamp, ticker))

    printElapsedTime(startTime)
      




