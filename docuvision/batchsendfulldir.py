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
# Args: (command line args. ex: python batchsendfulldir.py --types pdf --dir c:/test/ --loglevel DEBUG)
# - --types: pdf, png, or jpg. may include up to all 3 seperated by a comma. default=pdf,jpg,png
# - --dir: full or relative path to a directory on the current machine you'd like to post to DocuVision API. default=current directory
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
from pathlib import Path, PurePath
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
    default="0.9", type=str)
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
    "https://services.hank.ai/docuvision/v1/") #PROD
    #"https://services-dev.hank.ai/docuvision/v1/") #DEV

APITOKEN = os.environ.get('DOCUVISION_API_TOKEN', None) #PROD
#APITOKEN = os.environ.get('DOCUVISION_API_TOKEN_DEV', None) #DEV

SERVICE_NAME = os.environ.get('DOCUVISION_SERVICE_NAME', None) #PROD

if APITOKEN is None:
    logging.error("DOCUVISION_API_TOKEN environment variable is not set. Aborting")
    sys.exit()
if SERVICE_NAME is None:
    logging.error("DOCUVISION_SERVICE_NAME environment variable is not set. Aborting")
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
    presignedurl_details = None
    try:
        headers = {"x-api-key": APITOKEN }
        resp = None
        resp = requests.request(
            method="POST",
            url=f"{APIADDRESS}upload-locations", #docuvision/v1/tasks/7",
            headers=headers,
            timeout=timeout
        )
        presignedurl_details = resp.json()
        logging.debug(f"Signed url for upload acquired, status code {resp.status_code}, details={presignedurl_details}")
    except Exception as e:
        logging.error("in hankai_get_presigned_url()", e)
    return presignedurl_details

#STEP 2.
# will post a file to a presigned s3 url
# dv_file to be a dictionary as created by loadFile() function
# presigned_url (json object, retrieved from hankai_get_presigned_url()) from s3
# returns 1 if successful (i.e. status code==200) or 0 if other status code or error caught
def hankai_post_file(dv_file, presignedurl_details, timeout=120):
    try:
        files = {'file': dv_file['base64encoded']}
        resp = None
        resp = requests.post(
            url=presignedurl_details.get('url'), #docuvision/v1/tasks/7",
            data=presignedurl_details.get('fields'),
            files=files,
            timeout=timeout
        )
        if resp.status_code<300:
            return 1
        else: logging.warning(f"Bad status code ({resp.status_code}) when uploading {dv_file['filepath'].name}. {resp.content}")
    except Exception as e:
        logging.error(f"posting {dv_file['filepath'].name} to {presignedurl_details}", e)
    return 0    

#STEP 3.
# submits the job to the api after having successfully uploaded the file to process to s3 in prior 2 steps
# expects dv_file to be a dictionary as created by loadFile() function
# timeout is in seconds
# writes the newly created 'jobid filepath' as a line in pendingjobs.docuvision file in current directory
# returns job id (int)
def hankai_submit_job(dv_file, presignedurl_details, args, timeout=60):
    try:
        headers = {"x-api-key": APITOKEN }
        req = {
            "name": "customername_job_x",
            "request": {
                "service": SERVICE_NAME,
                "document": {
                    "name": dv_file.get('filepath').name,
                    "dataType": "blob",
                    "encodingType": "base64/utf-8",
                    "mimeType": dv_file.get('filepath').suffix[1:], #removes the leading period
                    "model": args.model, #modify this to the model you want to consume
                    "confidenceInterval": float(args.confidence), #modify this to your liking
                    "isPerformOCR": True, #if set your result will have OCR'd words and bounding boxes
                    "sizeBytes": dv_file.get('length'),
                    "md5Sum": dv_file.get('md5'),
                    "dataKey": presignedurl_details['fields']['key'],
                }
            }
        }
        resp = None
        resp = requests.request(
            method="POST",
            url=f"{APIADDRESS}tasks/", #docuvision/v1/tasks/7",
            headers=headers,
            json=req,
            timeout=timeout
        )
        rjson = json.loads(resp.content)
        req_id = rjson.get("id")
        req_meta = rjson.get("metadata", {})
        if 'apiKey' in req_meta.keys(): req_meta['apiKey']='hidden'
        logging.info(f"Job posted. status={resp.status_code}. id={req_id}. file={dv_file['filepath'].name}. respmeta={req_meta}")

        if req_id is not None and resp.status_code==200: 
            #write the job id of the newly created job to local file 
            with open('pendingjobs.docuvision', 'a') as f:
                f.write(f"{req_id} {dv_file['filepath']}\n")
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
        url=f"{APIADDRESS}tasks/{jobid}",
        headers=headers
    )
    return resp

#STEP 5.
#returns 'completed' if the job is complete, 'inprogress' if not found or in progress, or 'error' if completed but state == error
def hankai_check_job_complete(jobid, resp):
    try:
        if resp.status_code==404:
            return '404error'
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
def hankai_write_json_results(jobid, filepath, apiresponse, completedstate):
    dfP = Path(filepath)
    jsonfp = dfP.parent / (dfP.stem + f'_{jobid}.{completedstate}.json')
    logging.debug(f"Writing api response for jobid={jobid} to {jsonfp}")
    with open(jsonfp, 'w') as f:
        jsonapir = json.loads(apiresponse.content)
        jsonapir['metadata']['apiKey']="{}...".format(jsonapir['metadata']['apiKey'][:15])
        json.dump(jsonapir, f, indent=2)

#checks if a filepathstem_xxx.completed.json file exists for a given file
def checkForCompletedJson(filepath):
    dfP = Path(filepath)
    jsonps = dfP.parent.rglob("{}*.json".format(dfP.stem))
    for jsonp in jsonps:
        if jsonp.stem.endswith('.completed'): return 1
    return 0

#loads the file at filepath and base64 encodes it, returning a dictionary of items needed for posting to docuvision API
def loadFile(filepath):
    with open(filepath, 'rb') as f: doc_contents = f.read()
    doc_md5 = hashlib.md5(doc_contents).hexdigest()
    doc_base64 = base64.b64encode(doc_contents)
    doc_length = len(doc_contents)
    logging.debug(f"File loaded. {filepath}")
    return {'filepath': Path(filepath), 'contents': doc_contents, 'base64encoded':doc_base64, 'md5': doc_md5, 'length': doc_length}

#pendingjobs is a list of {jobid, filepath} objects that are still pending (optional)
def postJobs(args, pendingjobs=[]):
    #go through each filetype in the directory, recursively (thus rglob), and send files to the api
    #dirP = Path(args.dir+'/')
    dirP = Path(PurePath(Path.cwd(), args.dir)) #will allow for relative paths AND absolute paths

    newjobids = []
    pendingjobs_filepaths = [x.get('filepath') for x in pendingjobs]
    logging.info("SEND documents for processing requested. Starting ...")
    for type in args.types.split(','):
        files = list(dirP.rglob(f"*.{type}"))
        logging.info("Processing {:,} {}s in {} ...".format(len(files), type, args.dir))
        for file in files:
            # if we weren't asked to reprocess all files AND if a completed json file already exists, move on to next file for processing
            if not args.reprocess:
                if checkForCompletedJson(file):
                    logging.debug(f"Skipping already processed file. {file.name}")
                    continue
                if str(file) in pendingjobs_filepaths:
                    logging.debug(f"Skipping file already sent, in pending state. {file.name}")
                    continue
            logging.info(f"Processing {file.name}")
            #load the file bytes and hash it
            dv_file = loadFile(file)
            #step 1. get presigned url for s3 file upload
            presignedurl_details = hankai_get_presigned_url()
            if presignedurl_details is None: continue #failed to get signed url. go to next file
            #step 2.
            if not hankai_post_file(dv_file, presignedurl_details): continue #failed to post file to signed url. go to next file
            #step 3. 
            jobid = hankai_submit_job(dv_file, presignedurl_details, args)
            if jobid is not None:
                newjobids.append(jobid)
        logging.info(f"Done sending {type}s from {args.dir}.")

def getJobs(args, retries=0, retrydelay=60):
    logging.info("GET document results requested. Starting ...")
    pendingjobs = []
    #read in the 'pendingjobs.docuvision' file and get in-progress (jobid filepath) from it
    logging.debug("Reading pending jobs from pendingjobs.docuvision ...")
    pendingjobfileP = Path('pendingjobs.docuvision')
    if not pendingjobfileP.exists():
        logging.info("No pending inprogress jobs to process")
    else:
        #open up the pendingjobs.docuvision file and load up jobs we are waiting on
        with open(pendingjobfileP, 'r') as f:
            for line in f.readlines():
                if line.strip()=="": continue #skip empty lines
                logging.debug(line.strip())
                jobid, filepath = line.split(" ", 1)
                pendingjobs.append({'jobid':jobid, 'filepath':filepath.strip()})

        MAXRETRIES = retries+1
        RETRYDELAY = retrydelay
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
            if len(pendingjobs)>0: logging.debug("Writing pending jobs ({}) still in progress to pendingjobs.docuvision ...".format([x['jobid'] for x in pendingjobs]))
            with open(pendingjobfileP, 'w') as f:
                for pj in pendingjobs:
                    f.write(f"{pj['jobid']} {pj['filepath']}\n")
            if len(pendingjobs)==0:
                break
            if i+1 < MAXRETRIES:
                logging.info(f"Sleeping for {RETRYDELAY} seconds then trying again. Try #{i} of {MAXRETRIES-1}.")
                time.sleep(RETRYDELAY)
    return pendingjobs 


pendingjobs = []

if args.wtd.upper() == 'BOTH' and not args.reprocess:
    pendingjobs = getJobs(args, retries=0, retrydelay=5) #try to get any finished jobs first

#%% SUBMIT JOBS
if args.wtd.upper() in ['POST','BOTH']:
    postJobs(args, pendingjobs)

#%% CHECK FOR JOB RESULTS
#if you're testing and want to change the loglevel dynamically use something like this
# logging.getLogger().setLevel(logging.DEBUG)
if args.wtd.upper() in ['GET','BOTH']:
    pendingjobs = getJobs(args, retries=5, retrydelay=60)



if len(pendingjobs)>0:
    print("{:,} jobs still pending {}".format(len(pendingjobs), pendingjobs))
    logging.warning("{:,} jobs still pending {}".format(len(pendingjobs), pendingjobs))
logging.info("DOCUVISION SCRIPT COMPLETE")
print("DOCUVISION SCRIPT COMPLETE")

#%%

