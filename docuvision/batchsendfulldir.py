#%%
#####
## Hank.ai Inc
# (c) 2022
# 
# Hank.ai DocuVision API Sample Code
#
# Batch send all files of given types in the given directory and get results
# 
# Prerequisites:
# - Set ENV variables on the machine running this script. 
#   - Set 'DOCUVISION_API_TOKEN' to the token given to you by hank.ai during signup
#   - Set 'DOCUVISION_API_ADDRESS' to the url given to you by hank.ai during signup
# - Python 3.7.7 or greater installed
# Args: (command line args. ex: python batchsendfulldir.py --types pdf --dir c:/test/ --loglevel DEBUG)
# - --types: pdf, png, or jpg. may include up to all 3 seperated by a comma. default=pdf,jpg,png
# - --dir: full path to a directory on the current machine you'd like to post to DocuVision API. default=current directory
# - --loglevel: how verbose you want to logging to be to the docuvision.log file
# Results:
# - will save a .json file alongside each item posted to the DocuVision API once results are received
# - will append logging info to docuvision.log in same dir as this script is run from
# - will save any pending jobs that are still in progress or have not resulted by the end of the script as (jobid filepath) lines in pendingjobs.docuvision

import requests, hashlib, json, os, logging, sys, datetime, argparse, time
from pathlib import Path
from encodings.base64_codec import base64

#get command line parameters passed
ap = argparse.ArgumentParser("docuvision_api_sample", epilog="For help contact support@hank.ai") #, exit_on_error=False)
ap.add_argument("--types", help="pdf, png, or jpg. may include up to all 3 seperated by a comma. ", 
    default="pdf,png,jpg", type=str)
ap.add_argument("--dir", help="full path to a directory on the current machine to process", 
    default=".", type=str)
ap.add_argument("--loglevel", help="Logging level. Options are DEBUG, INFO, WARNING, ERROR, CRITICAL.", 
    default="DEBUG", type=str)

args, unknown = ap.parse_known_args()
if args.dir == '.': args.dir=os.path.dirname(os.path.realpath(__file__))
print("DOCUVISION SCRIPT STARTED")
print("args = ", args)

#setup logging
logging.basicConfig(
    filename='docuvision.log', 
    filemode='a', 
    level=args.loglevel.upper(),
    format='%(asctime)s:%(levelname)s:%(message)s')
logging.info("STARTING NEW JOB. {}".format(datetime.datetime.now()))
logging.info("Processing {} filetypes in {} ...".format(args.types, args.dir))

#%% SETUP AUTH
#make sure token and address are set in env vars (will use the default address here if not)
APIADDRESS = os.environ.get('DOCUVISION_API_ADDRESS', 
    "https://services.hank.ai/docuvision/v1/tasks/") #PROD
    #"https://services-dev.hank.ai/docuvision/v1/tasks/") #DEV

APITOKEN = os.environ.get('DOCUVISION_API_TOKEN', None)
if APITOKEN is None:
    logging.error("DOCUVISION_API_TOKEN environment variable is not set. Aborting")
    sys.exit()
if APIADDRESS is None:
    logging.error("DOCUVISION_API_ADDRESS environment variable is not set. Aborting")
    sys.exit()
if APIADDRESS[-1] != "/": APIADDRESS+="/"
logging.info("APIADDRESS loaded. Using {}".format(APIADDRESS))
logging.info("APITOKEN loaded. Using {}...".format(APITOKEN[:15]))


#expects filepath to be a Path() object
#timeout is in seconds
#writes the newly created 'jobid filepath' as a line in pendingjobs.docuvision file in current directory
#returns job id (int)
def hankai_submit_job(filepath, timeout=60):
    with open(file, 'rb') as f:
        raw_bytes = f.read()
        enc_bytes = base64.encodebytes(raw_bytes)
    enc_string = enc_bytes.decode("utf-8")
    md5_sum = hashlib.md5(raw_bytes).hexdigest()
    headers = {"x-api-key": APITOKEN }
    req = {
        "name": "customername_job_x",
        "request": {
            "document": {
                "name": filepath.name,
                "dataType": "blob",
                "encodingType": "base64/utf-8",
                "mimeType": filepath.suffix[1:], #removes the leading period
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
        url=f"{APIADDRESS}", #docuvision/v1/tasks/7",
        headers=headers,
        data=json.dumps(req),
        timeout=timeout
    )
    rjson = json.loads(resp.content)
    print(resp.content)
    req_id = rjson.get("id")
    req_meta = rjson.get("metadata", {})
    if 'apiKey' in req_meta.keys(): req_meta['apiKey']='hidden'
    logging.info(f"Job posted. status={resp.status_code}. id={req_id}. file={filepath.name}. respmeta={req_meta}")

    if req_id is not None: 
        #write the job id of the newly created job to local file 
        with open('pendingjobs.docuvision', 'a') as f:
            f.write(f"{req_id} {filepath}\n")
    return req_id

#gets the api response for a given jobid
def hankai_get_results(jobid):
    logging.debug("Getting {}{}".format(APIADDRESS, jobid))
    headers = {"x-api-key": APITOKEN }
    resp = requests.request(
        method="GET",
        url=f"{APIADDRESS}{jobid}",
        headers=headers
    )
    return resp

#returns 1 if the job is complete (even if state is error) or 0 if not found or in progress
def hankai_check_job_complete(resp):
    try:
        rjson= json.loads(resp.content)
        if (rjson.get("state") and rjson.get("state") != "In-Progress"):
            return 1
    except: 
        logging.error(f"in hank_check_job_complete(resp). resp={resp}")
    return 0

#writes out the results to a .json file of same base filestem+_jobid as original file
def hankai_write_json_results(jobid, document_filepath, apiresponse):
    dfP = Path(document_filepath)
    jsonfp = dfP.parent / (dfP.stem + f'_{jobid}.json')
    logging.debug(f"Writing api response for jobid={jobid} to {jsonfp}")
    with open(jsonfp, 'w') as f:
        jsonapir = json.loads(apiresponse.content)
        jsonapir['metadata']['apiKey']="{}...".format(jsonapir['metadata']['apiKey'][:15])
        json.dump(jsonapir, f, indent=2)

#%% SUBMIT JOBS
if 1:
    #go through each filetype in the directory, recursively (thus rglob), and send files to the api
    dirP = Path(args.dir+'/')
    newjobids = []
    for type in args.types.split(','):
        files = list(dirP.rglob(f"*.{type}"))
        logging.info("Processing {:,} {} in {} ...".format(len(files), type, args.dir))
        for file in files:
            jobid = hankai_submit_job(file)
            newjobids.append(jobid)
        logging.info(f"Done sending {type}s from {args.dir}.")

#%% CHECK FOR JOB RESULTS

#if you're testing and want to change the loglevel dynamically use something like this
# logging.getLogger().setLevel(logging.DEBUG)

pendingjobs = []
#read in the 'pendingjobs.docuvision' file and get in-progress (jobid filepath) from it
logging.debug("Reading pending jobs from pendingjobs.docuvision ...")
with open('pendingjobs.docuvision', 'r') as f:
    for line in f.readlines():
        logging.debug(line.strip())
        jobid, filepath = line.split(" ", 1)
        pendingjobs.append({'jobid':jobid, 'filepath':filepath.strip()})

MAXRETRIES = 10
RETRYDELAY = 30
logging.info("{:,} pending jobs still in progress. Checking on them now ...".format(len(pendingjobs)))

for i in range(1, MAXRETRIES+1):
    for pj in pendingjobs:
        time.sleep(1) 
        logging.debug("Retrieving jobid {}".format(pj['jobid']))
        res = hankai_get_results(pj['jobid'])
        complete = hankai_check_job_complete(res)
        if complete:
            hankai_write_json_results(pj['jobid'], pj['filepath'], res)
            logging.info(f"Completed jobid={pj['jobid']}")
            pendingjobs.remove(pj)
    if len(pendingjobs)==0:
        break
    logging.info("{:,} jobs still in progress.".format(len(pendingjobs)))
    logging.info(f"Sleeping for {RETRYDELAY} seconds then trying again. Try #{i} of {MAXRETRIES}.")
    time.sleep(RETRYDELAY)

#overwrite the pending jobs file with the ones still pending
logging.debug("Writing pending jobs ({}) still in progress to pendingjobs.docuvision ...".format([x['jobid'] for x in pendingjobs]))
with open('pendingjobs.docuvision', 'w') as f:
    for pj in pendingjobs:
        f.write(f"{pj['jobid']} {pj['filepath']}\n")

logging.info("DOCUVISION SCRIPT COMPLETE")

#%%

