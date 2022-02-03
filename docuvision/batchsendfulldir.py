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
import requests, hashlib, json, os, logging, sys, datetime, argparse
from pathlib import Path
from encodings.base64_codec import base64

#expects filepath to be a Path() object
#timeout is in seconds
def hankai_submit_job(filepath, timeout=10):
    with open(file, 'rb') as f:
        raw_bytes = f.read()
        enc_bytes = base64.encodebytes(raw_bytes)
    enc_string = enc_bytes.decode("utf-8")
    md5_sum = hashlib.md5(raw_bytes).hexdigest()
    
    req = {
        "name": "customername_job_x",
        "request": {
            "securityToken": APITOKEN,
            "document": {
                "name": filepath.name,
                "dataType": "blob",
                "encodingType": "base64/utf-8",
                "mimeType": filepath.suffix,
                "model": "base-medrec-anesthesia", #modify this to the model you want to consume
                "confidenceInterval": 0.7, #modify this to your liking
                "isPerformOCR": True, #if set your result will have OCR'd words and bounding boxes
                "sizeBytes": len(enc_bytes),
                "md5Sum": md5_sum,
                "data": enc_string,
            }
        }
    }
    
    resp = requests.request(
        method="POST",
        url=f"{APIADDRESS}docuvision/v1/tasks",
        data=json.dumps(req),
        timeout=timeout
    )
    rjson = json.loads(resp.content)
    print(resp.content)
    req_id = rjson.get("id")
    logging.info(f"Job posted (id={req_id}). Filename={filepath.name}")

#setup logging
logging.basicConfig(
    filename='docuvision.log', 
    filemode='a', 
    level=logging.DEBUG,
    format='%(asctime)s:%(levelname)s:%(message)s')
logging.info("STARTING NEW JOB. {}".format(datetime.datetime.now()))

#make sure token and address are set in env vars
os.environ['DOCUVISION_API_TOKEN'] = 'hankai_2020'
os.environ['DOCUVISION_API_ADDRESS'] = "https://ajy0m5kz2m.execute-api.us-east-1.amazonaws.com/prod/"
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
if args.dir == '.': args.dir=os.path.dirname(os.path.realpath(__file__))
logging.info("Processing {} filetypes in {} ...".format(args.types, args.dir))

### WIP ###
def hankai_get_results(jobid):    
    req_in_prog = True
    for i in range(max_retries):
        time.sleep(retry_delay)
        result = requests.request(
            method="GET",
            url=f"{base_url}docuvision/v1/tasks/{req_id}",
        )
        result_content = json.loads(result.content)
        print(f"Attempt {i + 1 }state: {result_content.get('state')}")
        if (result_content.get("state") and result_content.get("state") != "In-Progress"):
            break
    
    print("\n\nresponse received:\n\n")
    for key, val in result_content.items():
        if not key == "request":
            print(f"{key}: {val}")

#go through each filetype in the directory, recursively (thus rglob), and send files to the api
dirP = Path(args.dir+'/')
for type in args.types.split(','):
    files = list(dirP.rglob(f"*.{type}"))
    logging.info("Processing {:,} {} ...".format(len(files), type))
    for file in files:
        resp = hankai_submit_job(file)
        #handle polling for results next 

logging.info("JOB COMPLETED")

#%%
def hankai_submit_job(request, maxretries=10):
    import time
 
 
img_or_pdf_path = "C:/tmp/test-out/BetheaJanieMMRN000017902_pg1.png"
mime_type = "png"
base_url = "https://docuvision-api.hank.ai/"
max_retries, retry_delay  = 60, 5.0 # 60 total retries x 5 sec delay per retry = 5 minutes
 

 
resp = requests.request(
    method="POST",
    url=f"{base_url}docuvision/v1/tasks/",
    data=json.dumps(test_req),
    timeout=10,
)
req_content = json.loads(resp.content)
req_id = req_content.get("id")
print(f"request id: {req_id}")

