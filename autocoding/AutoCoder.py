#%%
#####
## Hank.ai Inc
# (c) 2022
# 
# Hank.ai AutoCoding API AutoCoder Class
#
# Class to prepare, submit, and retrieve autocoding jobs from Hank AutoCoding API
# 
# Prerequisites:
# - Set ENV variables on the machine running this script. 
#   - Set 'HANK_AUTOCODING_API_TOKEN' to the token given to you by hank.ai during signup
#   - Set 'HANK_AUTOCODING_API_ADDRESS' to the url given to you by hank.ai during signup
# - Python 3.7.7 or greater installed
#   - pandas, numpy, requests
#
# Notes:
# - If you use the dataframe based functions here we will still use temporary .json result files in case something bombs it can still pickup where it left off
# Resources:
# 
import pandas as pd
import numpy as np 
import requests, json, os, logging, datetime, time
from pathlib import Path
from tqdm import tqdm
tqdm.pandas()

APITOKEN = os.environ.get('HANK_AUTOCODING_API_TOKEN', None) #PROD
APIADDRESS = os.environ.get('HANK_AUTOCODING_API_ADDRESS', 
    "https://services.hank.ai/autocoding/v1/") #PROD
    #"https://services-dev.hank.ai/docuvision/v1/") #DEV

if APITOKEN is None:
    logging.error("HANK_AUTOCODING_API_TOKEN environment variable is not set. Aborting")
    sys.exit()
if APIADDRESS is None:
    logging.error("HANK_AUTOCODING_API_ADDRESS environment variable is not set. Aborting")
    sys.exit()
if APIADDRESS[-1] != "/": APIADDRESS+="/"

# import requests, json, os, logging, sys, datetime, argparse, time
# from pathlib import Path, PurePath

#static class (other than logger)
class AutoCoder: 
    def __init__(self, APITOKEN=None, APIADDRESS="https://services.hank.ai/autocoding/v1/", 
        logfile='autocoding.log', debuglevel="DEBUG", df_mapping=None, resultsdir = "results"):

        self.APITOKEN = APITOKEN
        self.APIADDRESS = APIADDRESS

        #this dict is used if you are calling hankai_submit_jobs_dataframe to map the dataframe column names (on the right) to the autocoding class keys
        if df_mapping is not None: self.df_mapping = df_mapping
        else: 
            self.df_mapping = {
                "CaseID": "CaseID", #unique id passed by the caller, will be returned as the "name" key in the api response. optional
                "SurgeryDescription": "SurgeryDescription", #short surgical description. optional, but recommended
                "DiagnosisDescription": "DiagnosisDescription", #short diagnosis description representing indication for procedure. optional, but recommended
                "ASAStatus": "ASAStatus", #asa disease status classification (1->6). optional, but recommended
                "PatientDOB": "PatientDOB", #patient's dob. format expected "YYYY-MM-DD HH:MM:SS". optional, but recommended
                "PatientSex": "PatientSex", #Male or Female. optinoal, but recommended
                "InsuranceCompany": "InsuranceCompany", #name of the patient's insurance company. optional
                "SurgeonName": "SurgeonName", #name of the surgeon, typically "Last, First MD" or similar
                "Emergency": "Emergency", #0 or 1. is this surgery classified as an emergency. optional
                "CaseStartTime": "CaseStartTime", #start time of the surgery. format expected "YYYY-MM-DD HH:MM:SS". optional
                "DateOfService": "DateOfService" #date of the surgery. format expected "YYYY-MM-DD 00:00:00". optional
            }
        self.resultsdir = resultsdir

        #setup logging
        logging.basicConfig(
            filename=logfile, 
            filemode='a', 
            level=debuglevel.upper(),
            format='%(asctime)s:%(levelname)s:%(message)s')
        logging.info("STARTING NEW JOB. {}".format(datetime.datetime.now()))
        #logging.info("Processing {} filetypes in {} ...".format(args.types, args.dir))
    
    #submit a payload for processing (autocoding)
    #stores jobids and the value in the "name" key in the payload to pendingjobs.autocoding
    #returns the jobid
    def hankai_submit_job(self, payload, timeout=60, quiet=True):
        if pd.isnull(payload): return -1
        headers = {"x-api-key": self.APITOKEN }
        resp = None
        resp = requests.request(
            method="POST",
            url=f"{self.APIADDRESS}tasks/", #docuvision/v1/tasks/7",
            headers=headers,
            json=payload,
            timeout=timeout
        )
        rjson = json.loads(resp.content)
        req_id = rjson.get("id")
        req_meta = rjson.get("metadata", {})
        if 'apiKey' in req_meta.keys(): req_meta['apiKey']='hidden'
        logging.info(f"Job posted. status={resp.status_code}. CaseID={payload.get('name')}. id={req_id}. respmeta={req_meta}")
        if not quiet: print(f"Job posted. status={resp.status_code}. CaseID={payload.get('name')}. id={req_id}")

        if req_id is not None and resp.status_code==200: 
            #write the job id of the newly created job to local file 
            with open('pendingjobs.autocoding', 'a') as f:
                f.write(f"{req_id} {payload.get('name')} \n")
        return req_id

    #helper function for dataframe that uses self.df_mapping to create a dict of values in mapped columns, as expected by createRequest 
    #note: will ignore any mapped items that don't have a matching column in dfrow
    def _autocodingMapper(self, dfrow):
        try:
            return {k:dfrow[v] for k,v in self.df_mapping.items() if v in dfrow.keys()}
        except: 
            return None

    #checks if a caseid already has a caseid_xxxjobidxxx.completed.json file
    def checkForCompletedCase(self, caseid):
        #checks if a caseid_xxx.completed.json file exists for a given file
        dirP = Path(self.resultsdir)
        jsonps = list(dirP.rglob("{}*.json".format(caseid)))[::-1] #reverse them to get the most recent one first   
        for jsonp in jsonps:
            if jsonp.stem.endswith('.completed'): return 1
        return 0

    #submit all rows in a dataframe as jobs to the autocoder
    #will use the self.df_mappings to map autocoder fields to the columns in your dataframe so modify those mappings based upon your dataframe
    #returns a pd.Series object that contains the jobids returned from the server
    def hankai_submit_job_dataframe(self, df, reprocess_completed_cases=False, timeout=60, quiet=True):
        df = df.copy()
        df['processed']=0
        if not reprocess_completed_cases:
            df['processed'] = df[self.df_mapping['CaseID']].apply(self.checkForCompletedCase)
        payloads = df[df['processed']==0].apply(lambda r: AutoCoder.createRequest(self._autocodingMapper(r)), axis=1)
        return payloads.progress_apply(lambda x: self.hankai_submit_job(x, quiet=quiet))


    #gets the api results for a given jobid
    def hankai_get_results(self, jobid):
        logging.debug("Getting {}{}".format(self.APIADDRESS, jobid))
        headers = {"x-api-key": self.APITOKEN }
        resp = requests.request(
            method="GET",
            url=f"{self.APIADDRESS}tasks/{jobid}",
            headers=headers
        )
        return resp

    #looks for pending autocoding jobs in the pending.autocoding file and then queries the server for status
    # if status is completed, will save the results to a .json file inside self.resultsdir directory
    # set retries to -1 to check indefinitely (technically 5,000 times) until all results are completed
    def getJobs(self, retries=0, retrydelay=60, quiet=True):
        logging.info("GET autocoding results requested. Starting ...")
        pendingjobs = []
        #read in the 'pendingjobs.autocoding' file and get in-progress (jobid CaseID) from it
        logging.debug("Reading pending jobs from pendingjobs.autocoding ...")
        pendingjobfileP = Path('pendingjobs.autocoding')
        if not pendingjobfileP.exists():
            logging.info("No pending inprogress jobs to process")
        else:
            #open up the pendingjobs.docuvision file and load up jobs we are waiting on
            with open(pendingjobfileP, 'r') as f:
                for line in f.readlines():
                    if line.strip()=="": continue #skip empty lines
                    logging.debug(line.strip())
                    jobid, caseid = line.split(" ", 1)
                    pendingjobs.append({'jobid':jobid, 'caseid':caseid.strip()})
            if retries==-1: MAXRETRIES = 5000
            else: MAXRETRIES = retries+1
            RETRYDELAY = retrydelay
            logging.info("{:,} pending jobs still in progress. Checking on them now ...".format(len(pendingjobs)))


            for i in range(1, MAXRETRIES+1):
                for pj in pendingjobs.copy():
                    time.sleep(1) #don't destroy the api
                    logging.debug("Retrieving jobid {}".format(pj['jobid']))
                    res = self.hankai_get_results(pj['jobid'])
                    completedstate = AutoCoder.hankai_check_job_complete(pj['jobid'], res)
                    if completedstate in ['completed', 'error']:
                        print(f"Jobid {pj['jobid']} complete! state={completedstate}")
                        AutoCoder.hankai_write_json_results(pj['jobid'], pj['caseid'], res, completedstate, subdir=self.resultsdir)
                        logging.info(f"Completed jobid={pj['jobid']}. state={completedstate}")
                        pendingjobs.remove(pj)
                    else:
                        print(f"jobid={pj['jobid']} for caseid={pj['caseid']} is still pending")

                logging.info("{:,} jobs still in progress.".format(len(pendingjobs)))
                #overwrite the pending jobs file with the ones still pending
                if len(pendingjobs)>0: logging.debug("Writing pending jobs ({}) still in progress to pendingjobs.autocoding ...".format([x['jobid'] for x in pendingjobs]))
                with open(pendingjobfileP, 'w') as f:
                    for pj in pendingjobs:
                        f.write(f"{pj['jobid']} {pj['caseid']}\n")
                if len(pendingjobs)==0:
                    break
                if i+1 < MAXRETRIES:
                    logging.info(f"Sleeping for {RETRYDELAY} seconds then trying again. Try #{i} of {MAXRETRIES}.")
                    if not quiet: print(f"-> Sleeping for {RETRYDELAY} seconds then trying again. Try #{i} of {MAXRETRIES}.")
                    time.sleep(RETRYDELAY)
        return pendingjobs 
    
    #returns a tuple, (the json loaded response, the json filename)
    def loadCaseResults(self, caseid):
        #checks if a caseid_xxx.completed.json file exists for a given file
        dirP = Path(self.resultsdir)
        jsonps = list(dirP.rglob("{}*.json".format(caseid)))[::-1] #reverse them to get the most recent one first   
        for jsonp in jsonps:
            if jsonp.stem.endswith('.completed'):
                with open(jsonp) as f:
                    return json.load(f), jsonp.name
        return np.nan, np.nan

    #takes a dataframe df and loads the top cpt, asa, and icd to new columns
    #uses the CaseID from self.df_mapping to determine which .json results file to load for the row
    #returns the original df plus the new columns, including the jobid in autocoding_jobid column
    #entity is true or false. 
    #  if true, will add autocoding_*_entity columns to the df that contain the full entity 
    #  if false, will add 2 columns for each code type. one holding the code, the other holding the confidence
    def loadResultsToDataframe(self, df, entity=False):
        cr = df.progress_apply(lambda r: self.loadCaseResults(r[self.df_mapping['CaseID']]), result_type='expand', axis=1)
        responses = cr[0]
        filenames = cr[1]
        df['autocoding_jobid'] = responses.apply(lambda x: x['id'] if not pd.isnull(x) and 'id' in x.keys() else np.nan)
        df['autocoding_result_filename'] = filenames
        cpt_ents = responses.apply(AutoCoder.getTopCPT)
        asa_ents = responses.apply(AutoCoder.getTopASA)
        icd_ents = responses.apply(AutoCoder.getTopICD)
        if entity:
            df['autocoding_cpt_entity'] = cpt_ents
            df['autocoding_asa_entity'] = asa_ents
            df['autocoding_icd10_entity'] = icd_ents
        else:
            df['autocoding_cpt_code'] = cpt_ents.apply(lambda x: x['entityValue'] if not pd.isnull(x) and 'entityValue' in x.keys() else np.nan)
            df['autocoding_cpt_conf'] = cpt_ents.apply(lambda x: x['confidence'] if not pd.isnull(x) and 'confidence' in x.keys() else np.nan)
            df['autocoding_asa_code'] = asa_ents.apply(lambda x: x['entityValue'] if not pd.isnull(x) and 'entityValue' in x.keys() else np.nan)
            df['autocoding_asa_conf'] = asa_ents.apply(lambda x: x['confidence'] if not pd.isnull(x) and 'confidence' in x.keys() else np.nan)
            df['autocoding_icd10_code'] = icd_ents.apply(lambda x: x['entityValue'] if not pd.isnull(x) and 'entityValue' in x.keys() else np.nan)
            df['autocoding_icd10_conf'] = icd_ents.apply(lambda x: x['confidence'] if not pd.isnull(x) and 'confidence' in x.keys() else np.nan)
        return df
        

    ################ STATIC STUFF ###############
    sample_case_datas = [
        {
            "CaseID": "ABC123",
            "SurgeryDescription":"Left total hip arthroplasty complete (C-ARM)",
            "DiagnosisDescription":"hip pain and arthritis",
            "ASAStatus": "3",
            "PatientDOB":"1972-03-19 00:00:00",
            "PatientSex":"Female",
            "InsuranceCompany":"Commercial Insurance",
            "SurgeonName":"Cutter, Jimmy MD",
            "Emergency":"0",
            "CaseStartTime": "2020-10-30 11:19:00",
            "DateOfService": "2020-10-30 00:00:00"
        },
        {
            "CaseID": "DEF456",
            "SurgeryDescription":"tonsil removal & adenoidectomy and bmt",
            "DiagnosisDescription":"repeat ear infections",
            "ASAStatus": "1",
            "PatientDOB":"2019-03-19 00:00:00",
            "PatientSex":"Female",
            "InsuranceCompany":"Commercial Insurance",
            "SurgeonName":"Cutter, Jimmy MD",
            "Emergency":"0",
            "CaseStartTime": "2021-01-03 08:19:00",
            "DateOfService": "2021-01-03 00:00:00"
        }
    ]

    #returns an entity dict for the payload object in createRequest(...) from key:value pairs 
    def _createEntity(key, value):
        return {
            "content":value,
            "noteType":key,
            "inputType":"text",
            #"lastModifiedTime": "2021-05-13 14:45:49"
        }

    # takes a one-level dictionary with keys that hold values to be formatted into the autocoding spec format
    # returns the payload in the format expected by hankai_submit_job(...)
    def createRequest(caseInfo):
        entities = ["SurgeryDescription", "DiagnosisDescription", "SurgeonProcedureNote","AnesthesiaProcedureNote"]
        payload = {
            #"id":Null,
            "name":caseInfo.get("CaseID"),
            "request": {
                "service":"autocoding-1",
                "job":{
                    "input":{
                        "entities":[AutoCoder._createEntity(k,v) for k,v in caseInfo.items() if k in entities]
                    },
                    "patient_info": {
                        "patient": {
                            "dob": caseInfo.get("PatientDOB"),
                            "sex": caseInfo.get("PatientSex")
                        },
                        "insurance": [
                            {"company": caseInfo.get("InsuranceCompany")}
                        ]
                    },
                    "schedule": {
                        "asa": caseInfo.get("ASAStatus"),
                        "date": caseInfo.get("DateOfService"),
                        "startTime": caseInfo.get("CaseStartTime"),
                        "emergent": caseInfo.get("Emergency")
                    }
                }
            }
        }
        return payload

    #checks if a filepathstem_xxx.completed.json file exists for a given file
    def checkForCompletedJson(filepath):
        dfP = Path(filepath)
        jsonps = dfP.parent.rglob("{}*.json".format(dfP.stem))
        for jsonp in jsonps:
            if jsonp.stem.endswith('.completed'): return 1
        return 0

    #checks the resp json object status_code and returns one of:
    #   404error, error, completed, inprogress
    # returns 'completed' if the job is complete, 'inprogress' if not found or in progress, or 'error' if completed but state == error
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

    #writes out the results to a .json file of the form caseid_jobid.completedstate.json at same location as original file
    #pass a subdir string if you want to create a subdir in the current folder to store the json responses there
    def hankai_write_json_results(jobid, caseid, apiresponse, completedstate, subdir="results"):
        dfP = Path(caseid)
        dirP = dfP.parent / subdir
        dirP.mkdir(exist_ok=1)
        jsonfp = dirP / (dfP.stem + f'_{jobid}.{completedstate}.json')
        logging.debug(f"Writing api response for jobid={jobid} to {jsonfp}")
        with open(jsonfp, 'w') as f:
            jsonapir = json.loads(apiresponse.content)
            jsonapir['metadata']['apiKey']="{}...".format(jsonapir['metadata']['apiKey'][:15])
            json.dump(jsonapir, f, indent=2)
    
    #jsonresp is a full json object response from the autocoding api
    #returns the top surgical cpt code entity
    def getTopCPT(jsonresp):
        return AutoCoder.getTopCode('surgCPT', jsonresp)
    #jsonresp is a full json object response from the autocoding api
    #returns the top anesthesia cpt code entity
    def getTopASA(jsonresp):
        return AutoCoder.getTopCode('anesCPT', jsonresp)
    #jsonresp is a full json object response from the autocoding api
    #returns the top icd10 code entity
    def getTopICD(jsonresp):
        return AutoCoder.getTopCode('ICD10', jsonresp)
    
    #codetype options are surgCPT, anesCPT, and ICD10
    #jsonresp is a full json object response from the autocoding api
    #returns the first entity from the entities that matches type==codetype
    def getTopCode(codetype, jsonresp):
        for e in jsonresp['response']['result']['entities']:
            if 'type' in e.keys() and e['type'].lower() == codetype.lower():
                return e



#df['autocoding_jobid'] = df['jsondictforautocing'].apply(lambda x: hankai_submit_job(createRequest(x)), axis=1)

#ac = an AutoCoder object
def postJobs(ac, sample_case_datas):
    jobids = []
    for case_data in AutoCoder.sample_case_datas:
        payload = AutoCoder.createRequest(case_data)
        jobids.append(ac.hankai_submit_job(payload))
    return jobids

#################################
#### SAMPLE CODE FOR TESTING ####
#################################
#instantiate the AutoCoder class object with an apitoken and (optional) api endpoint address
ac = AutoCoder(APITOKEN=APITOKEN, APIADDRESS=APIADDRESS)
#create a pandas dataframe using sample data included in the class
df = pd.DataFrame(ac.sample_case_datas)
#attempt to load any existing results stored locally in results/ to the dataframe
df = ac.loadResultsToDataframe(df)
#submit the cases in the dataframe for autocoding
df['autocoding_jobid_justsent'] = ac.hankai_submit_job_dataframe(df, quiet=0, reprocess_completed_cases=1)
#retrieve the results for the posted jobs and store them locally as .json files in results/
ac.getJobs(retries=-1, retrydelay=10, quiet=False)
#load results stored locally in results/ to the dataframe
ac.loadResultsToDataframe(df)



#%%


#%%
def pulloutcodes(row):
    cpt = row['codingresults'].get('surgCPT')
    asa = cpt = row['codingresults'].get('anesCPT')
    return cpt, asa
#df[['predictedcpt', 'predictedasa']] = df.apply(pulloutcodes, axis=1, result_type="expand")


#%%

postJobs(ac, sample_case_datas)


#%%

pendingjobs = []
pendingjobs = ac.getJobs(retries=5, retrydelay=5) #try to get any finished jobs first




if len(pendingjobs)>0:
    print("{:,} jobs still pending {}".format(len(pendingjobs), pendingjobs))
    logging.warning("{:,} jobs still pending {}".format(len(pendingjobs), pendingjobs))
logging.info("AUTOCODING SCRIPT COMPLETE")
print("AUTOCODING SCRIPT COMPLETE")

#%%

