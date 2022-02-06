#%%
#####
## Hank.ai Inc
# (c) 2022
# 
# Hank.ai DocuVision API Sample Code
#
# Batch send all files of given types in the given directory and get results
# 
# Example command line command to process all pdfs in directory c:/test/
#   python batchsendfulldir.py --types pdf --dir c:/test/ --loglevel DEBUG
#
# Prerequisites:
# - Set ENV variables on the machine running this script. 
#   - Set 'DOCUVISION_API_TOKEN' to the token given to you by hank.ai during signup
#   - Set 'DOCUVISION_API_ADDRESS' to the url given to you by hank.ai during signup
# - Python 3.7.7 or greater installed
#   - boto3 'pip install boto3' (this is aws' library for interacting with s3. needed for uploading files to the signed url returned by first api call)
# Args: (command line args. ex: python batchsendfulldir.py --types pdf --dir c:/test/ --loglevel DEBUG)
# - --types: pdf, png, or jpg. may include up to all 3 seperated by a comma. default=pdf,jpg,png
# - --dir: full path to a directory on the current machine you'd like to post to DocuVision API. default=current directory
# - --loglevel: how verbose you want to logging to be to the docuvision.log file
# Results:
# - will save a .json file alongside each item posted to the DocuVision API once results are received
# - will append logging info to docuvision.log in same dir as this script is run from
# - will save any pending jobs that are still in progress or have not resulted by the end of the script as (jobid filepath) lines in pendingjobs.docuvision

# Resources:
# AWS S3 Presigned URL Upload Tutorial in Python
#   https://beabetterdev.com/2021/09/30/aws-s3-presigned-url-upload-tutorial-in-python/
#


import requests, hashlib, json, os, logging, sys, datetime, argparse, time
from pathlib import Path
from encodings.base64_codec import base64

#get command line parameters passed
ap = argparse.ArgumentParser("docuvision_api_sample", epilog="For help contact support@hank.ai") #, exit_on_error=False)
ap.add_argument("--wtd", help="What to do. ONLY send documents for processing (POST), ONLY check on inprogress jobs (GET), or BOTH", 
    default="BOTH", type=str)
ap.add_argument("--types", help="pdf, png, or jpg. may include up to all 3 seperated by a comma. ", 
    default="pdf,png,jpg", type=str)
ap.add_argument("--dir", help="full path to a directory on the current machine to process", 
    default=".", type=str)
ap.add_argument("--confidence", help="confidence level for the docuvision api. will only return fields with a confidence >= this float.", 
    default="0.80", type=str)
ap.add_argument("--model", help="pass a specific docuvision model to consume", 
    default="base-medrec-anesthesia", type=str)
ap.add_argument("--reprocess", help="if set, will reprocess all files in the given directory even if they have already been processed", 
    default=0, type=int)
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
APIADDRESS_GETSIGNEDURL = "https://services.hank.ai/docuvision/v1/getsignedposturl/"

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

#STEP 1.
# returns a presigned s3 bucket url that a file can be posted to
def hankai_get_presigned_url(timeout=20):
    surl = None
    try:
        headers = {"x-api-key": APITOKEN }
        req = {
            "filename": filepath.name
        }
        resp = None
        resp = requests.request(
            method="POST",
            url=f"{APIADDRESS_GETSIGNEDURL}", #docuvision/v1/tasks/7",
            headers=headers,
            timeout=timeout
        )
        rjson = json.loads(resp.content)
        surl = rjson.get("presignedurl")
    except Exception as e:
        logging.error("in hankai_get_presigned_url()", e)
    return surl

#STEP 2.
# will post a file given by filepath (a Path object) to a 
#  presigned url (string, retrieved from hankai_get_presigned_url()) from s3
# returns 1 if successful (i.e. status code==200) or 0 if other status code or error caught
def hankai_post_file(filepath, presignedurl, timeout=120):
    try:
        files = {'file': open(filepath, 'rb')}
        resp = None
        resp = requests.post(
            url=f"{presignedurl}", #docuvision/v1/tasks/7",
            files=files,
            timeout=timeout
        )
        if resp.status_code==200:
            return 1
        else: logging.warning(f"Bad status code ({resp.status_code}) when uploading {filepath.name}.")
    except Exception as e:
        logging.error(f"posting {filepath.name} to {presignedurl}", e)
    return 0    

#STEP 3.
# submits the job to the api after having successfully uploaded the file to process to s3 in prior 2 steps
# expects filepath to be a Path() object
# timeout is in seconds
# writes the newly created 'jobid filepath' as a line in pendingjobs.docuvision file in current directory
# returns job id (int)
def hankai_submit_job(filepath, timeout=60):
    try:
        with open(filepath, 'rb') as f:
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
                    "model": args.model, #modify this to the model you want to consume
                    "confidenceInterval": float(args.confidence), #modify this to your liking
                    "isPerformOCR": True, #if set your result will have OCR'd words and bounding boxes
                    "sizeBytes": len(enc_bytes),
                    "md5Sum": md5_sum,
                    "data": enc_string,
                }
            }
        }
        resp = None
        resp = requests.request(
            method="POST",
            url=f"{APIADDRESS}", #docuvision/v1/tasks/7",
            headers=headers,
            data=json.dumps(req),
            timeout=timeout
        )
        rjson = json.loads(resp.content)
        req_id = rjson.get("id")
        req_meta = rjson.get("metadata", {})
        if 'apiKey' in req_meta.keys(): req_meta['apiKey']='hidden'
        logging.info(f"Job posted. status={resp.status_code}. id={req_id}. file={filepath.name}. respmeta={req_meta}")

        if req_id is not None: 
            #write the job id of the newly created job to local file 
            with open('pendingjobs.docuvision', 'a') as f:
                f.write(f"{req_id} {filepath}\n")
        return req_id
    except Exception as e:
        logging.error(f"{e}. {resp}")
    return None

#STEP 4.
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

#STEP 5.
#returns 'completed' if the job is complete, 'inprogress' if not found or in progress, or 'error' if completed but state == error
def hankai_check_job_complete(jobid, resp):
    try:
        rjson= json.loads(resp.content)
        if (rjson.get("state")):
            logging.debug(f"jobid {jobid} state={rjson.get('state')}")
            if rjson.get("state").lower() == 'error': 
                logging.warning(f"API response shows error state for jobid={jobid}")
                return 'error'
            if rjson.get("state").lower() == "completed":
                return 'completed'
    except Exception as e: 
        logging.error(f"in hank_check_job_complete(resp). error={e} resp={resp}")
    return "inprogress"

#STEP 6.
#writes out the results to a .json file of the form filestem+_jobid.completedstate.json at same location as original file
def hankai_write_json_results(jobid, document_filepath, apiresponse, completedstate):
    dfP = Path(document_filepath)
    jsonfp = dfP.parent / (dfP.stem + f'_{jobid}.{completedstate}.json')
    logging.debug(f"Writing api response for jobid={jobid} to {jsonfp}")
    with open(jsonfp, 'w') as f:
        jsonapir = json.loads(apiresponse.content)
        jsonapir['metadata']['apiKey']="{}...".format(jsonapir['metadata']['apiKey'][:15])
        json.dump(jsonapir, f, indent=2)

#checks if a filepathstem_xxx.completed.json file exists for a given file
def checkForCompletedJson(filepath):
    jsonps = filepath.parent.rglob("{}*.json".format(filepath.stem))
    for jsonp in jsonps:
        if jsonp.stem.endswith('.completed'): return 1
    return 0

#%% SUBMIT JOBS
if args.wtd.upper() in ['POST','BOTH']:
    #go through each filetype in the directory, recursively (thus rglob), and send files to the api
    dirP = Path(args.dir+'/')
    newjobids = []
    logging.info("SEND documents for processing requested. Starting ...")
    for type in args.types.split(','):
        files = list(dirP.rglob(f"*.{type}"))
        logging.info("Processing {:,} {}s in {} ...".format(len(files), type, args.dir))
        for file in files:
            # if we weren't asked to reprocess all files AND if a completed json file already exists, move on to next file for processing
            if not args.reprocess and checkForCompletedJson(file):
                logging.debug(f"Skipping already processed file {file.name}")
                continue
            logging.info(f"Processing {file.name}")
            #step 1. get presigned url for s3 file upload
            surl = hankai_get_presigned_url()
            if surl is None: continue #failed to get signed url. go to next file
            #step 2.
            if not hankai_post_file(file, surl): continue #failed to post file to signed url. go to next file
            #step 3. 
            jobid = hankai_submit_job(file)
            if jobid is not None:
                newjobids.append(jobid)
        logging.info(f"Done sending {type}s from {args.dir}.")

#%% CHECK FOR JOB RESULTS

#if you're testing and want to change the loglevel dynamically use something like this
# logging.getLogger().setLevel(logging.DEBUG)
if args.wtd.upper() in ['GET','BOTH']:
    logging.info("GET document results requested. Starting ...")
    pendingjobs = []
    #read in the 'pendingjobs.docuvision' file and get in-progress (jobid filepath) from it
    logging.debug("Reading pending jobs from pendingjobs.docuvision ...")
    pendingjobfileP = Path('pendingjobs.docuvision')
    if not pendingjobfileP.exists():
        logging.info("No pending inprogress jobs to process")
    else:
        with open(pendingjobfileP, 'r') as f:
            for line in f.readlines():
                if line.strip()=="": continue #skip empty lines
                logging.debug(line.strip())
                jobid, filepath = line.split(" ", 1)
                pendingjobs.append({'jobid':jobid, 'filepath':filepath.strip()})

        MAXRETRIES = 10
        RETRYDELAY = 30
        logging.info("{:,} pending jobs still in progress. Checking on them now ...".format(len(pendingjobs)))

        for i in range(1, MAXRETRIES+1):
            for pj in pendingjobs:
                time.sleep(1) #don't destroy the api
                logging.debug("Retrieving jobid {}".format(pj['jobid']))
                res = hankai_get_results(pj['jobid'])
                completedstate = hankai_check_job_complete(pj['jobid'], res)
                if completedstate in ['completed', 'error']:
                    print(f"Jobid {pj['jobid']} complete! state={completedstate}")
                    hankai_write_json_results(pj['jobid'], pj['filepath'], res, completedstate)
                    logging.info(f"Completed jobid={pj['jobid']}. state={completedstate}")
                    pendingjobs.remove(pj)

            logging.info("{:,} jobs still in progress.".format(len(pendingjobs)))
            #overwrite the pending jobs file with the ones still pending
            logging.debug("Writing pending jobs ({}) still in progress to pendingjobs.docuvision ...".format([x['jobid'] for x in pendingjobs]))
            with open(pendingjobfileP, 'w') as f:
                for pj in pendingjobs:
                    f.write(f"{pj['jobid']} {pj['filepath']}\n")
            if len(pendingjobs)==0:
                break
            logging.info(f"Sleeping for {RETRYDELAY} seconds then trying again. Try #{i} of {MAXRETRIES}.")
            time.sleep(RETRYDELAY)



logging.info("DOCUVISION SCRIPT COMPLETE")
print("DOCUVISION SCRIPT COMPLETE")

#%%

