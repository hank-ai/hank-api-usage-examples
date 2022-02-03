#%%
#####
## Hank.ai Inc
# (c) 2022
# 
# Hank.ai DocuVision API Sample Code
#
# Batch send all files of given types in the given directory 
# 
# Prerequisites:
# - Set ENV variables on the machine running this script. 
#   - Set 'DOCUVISION_API_TOKEN' to the token given to you by hank.ai during signup
#   - Set 'DOCUVISION_API_ADDRESS' to the url given to you by hank.ai during signup
# - Python 3.7.7 or greater installed
# - Python packages pandas and numpy. Install via 'pip install pandas' and 'pip install numpy'
# Args: 
# - types: pdf, png, or jpg. may include up to all 3 seperated by a comma. default=pdf,jpg,png
# - dir: full path to a directory on the current machine you'd like to post to DocuVision API. default=current directory
#
# Results:
# - will save a .json file alongside each item posted to the DocuVision API once results are received
# - will append logging info to docuvision.log in same dir as this script is run from

import pandas as pd
import numpy as np
import requests
import json, os, logging, sys, datetime, argparse
from pathlib import Path

#setup logging
logging.basicConfig(
    filename='docuvision.log', 
    filemode='a', 
    level=logging.DEBUG,
    format='%(asctime)s:%(levelname)s:%(message)s')
logging.info("STARTING NEW JOB. {}".format(datetime.datetime.now()))

#make sure token and address are set in env vars
os.environ['DOCUVISION_API_TOKEN'] = 'abc'
os.environ['DOCUVISION_API_ADDRESS'] = 'https://api.hank.ai/docuvision/'
APITOKEN = os.environ.get('DOCUVISION_API_TOKEN', None)
APIADDRESS = os.environ.get('DOCUVISION_API_ADDRESS', None)
if APITOKEN is None:
    logging.error("DOCUVISION_API_TOKEN environment variable is not set. Aborting")
    sys.exit()
if APIADDRESS is None:
    logging.error("DOCUVISION_API_ADDRESS environment variable is not set. Aborting")
    sys.exit()

#get command line parameters passed
ap = argparse.ArgumentParser("docuvision_api_sample", epilog="For help contact support@hank.ai") #, exit_on_error=False)
ap.add_argument("--types", help="pdf, png, or jpg. may include up to all 3 seperated by a comma. ", 
    default="pdf,png,jpg", type=str)
ap.add_argument("--dir", help="full path to a directory on the current machine to process", 
    default=".", type=str)
args, unknown = ap.parse_known_args()
types = args.types
dir = args.dir
logging.info("Processing {} filetypes in {} ...".format(types, dir))

dirP = Path(dir)
for type in types.split(','):
    files = list(dirP.rglob("*.{type}"))
    logging.info("Processing {:,} {} ...".format(len(files), type))


logging.info("JOB COMPLETED")

#%%
def hankai_submit_job(request, maxretries=10):
    subkey = "b9c2b66b0c7b4e5788aa7a1b7ec7922c"
    endpoint = "https://cognitiveservices-docvision.cognitiveservices.azure.com/"
    from azure.cognitiveservices.vision.computervision import ComputerVisionClient
    from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
    from azure.cognitiveservices.vision.computervision.models import VisualFeatureTypes
    from msrest.authentication import CognitiveServicesCredentials
    from azure.cognitiveservices.vision.computervision.operations import ComputerVisionClientOperationsMixin

    import time
    wordRects = []
    lineRects = []
    meta = {}
    rdict = {'meta':{}, 'lines': [], 'words':[]}
    #print("Getting OCR results from azure ...")

    computervision_client = ComputerVisionClient(endpoint, CognitiveServicesCredentials(subkey))
    errorcount = 0 #limit tries to 5 if error is encountered (typically related to too many requests too quickly)
    while errorcount<maxretries:
        try:
            with open(imgpath, 'rb') as f:
                recognize_results = computervision_client.read_in_stream(f, raw=True)
            break
        except Exception as e:
            errorcount+=1
            if str(e).find('Too Many Requests')>=0: 
                print("   -> Too Many Requests to azure ocr read_in_stream. waiting 2 seconds then trying again (tries={:,})".format(errorcount))
                time.sleep(2)
            else:
                print("   -> Error requesting azure ocr read_in_stream. ({}). waiting 2 seconds then trying again (tries={:,})".format(e, errorcount))
                time.sleep(2)

    if maxretries==errorcount: 
        return rdict

    oplocrem = recognize_results.headers['Operation-Location']
    opid = oplocrem.split("/")[-1]
    getres=None
    errorcount=0
    while True and errorcount<maxretries:
        try:
            getres = computervision_client.get_read_result(opid)
            if getres.status not in ['notStarted','running']:
                #print("problem. ", getres.status)
                break
            time.sleep(0.8)
        except Exception as e:
            errorcount+=1
            if str(e).find('Too Many Requests')>=0: 
                print("   -> Too Many Requests to azure ocr get_read_result. waiting 2 seconds then trying again (tries={:,})".format(errorcount))
                time.sleep(2)
            else:
                print("   -> Error requesting azure ocr get_read_result. ({}). waiting 2 seconds then trying again (tries={:,})".format(e, errorcount))
                time.sleep(2)

    if getres is None:
        print("Never got a succeeded response status code from azure ocr. gonna move along ...")
    elif getres.status == OperationStatusCodes.succeeded:
        if len(getres.analyze_result.read_results)==0:
            print("No results returned from azure.")

        for text_result in getres.analyze_result.read_results:
            meta = {'angle': text_result.angle, 'width':text_result.width, 'height':text_result.height, 'unit':text_result.unit, 'language':text_result.language}
            for line in text_result.lines:
                lineWordRects = []
                bb = line.bounding_box
                #convert polygon coords to rectangle coords
                #[0tlx, 1tly, 2trx, 3try, 4brx, 5bry, 6blx, 7bly]
                l = min(bb[0], bb[6])
                t = min(bb[1], bb[3])
                h = max(bb[5], bb[7]) - t
                w = max(bb[2], bb[4]) - l
                lineRect = Rectangle(l, t, w, h, yfrom='top', pageWidth=text_result.width, pageHeight=text_result.height)
                for word in line.words:
                    #print(word.text)
                    #print(word.bounding_box)
                    bb = word.bounding_box

                    #convert polygon coords to rectangle coords
                    #[0tlx, 1tly, 2trx, 3try, 4brx, 5bry, 6blx, 7bly]
                    l = min(bb[0], bb[6])
                    t = min(bb[1], bb[3])
                    h = max(bb[5], bb[7]) - t
                    w = max(bb[2], bb[4]) - l
                    lineWordRects.append(((Rectangle(l, t, w, h, yfrom='top', pageWidth=text_result.width, pageHeight=text_result.height), word.text, word, line, meta)))
                rdict['words'] += lineWordRects
                rdict['lines'].append((lineRect, line.text, line, lineWordRects, meta))
            rdict['meta'] = meta
    return rdict
