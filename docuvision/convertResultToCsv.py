#%%
#####
## Hank.ai Inc
# (c) 2022
# 
# Hank.ai DocuVision API Sample Code
#
# Convert DocuVision json response files into CSV format. 
#   If --json not passed, will convert ALL .json files in the given --dir to .csv format (or current dir if --dir not passed)
#   if --json is passed, will convert ONLY that .json file
#
# Example command line command to convert all jsons in directory c:/test/ and create seperate csvs for each unique pid
#   python convertResultToCSV.py --dir c:/test/ --splitonpid 1 --loglevel DEBUG 
#
# Prerequisites:
# - Python 3.7.7 or greater installed
#   - pandas (pip install pandas)
# Args: (command line args)
# - --dir: (optional) full path to a directory on the current machine you'd like to post to DocuVision API. default=current directory
# - --splitonpid: (optional) if 1 will create, in addition to main 
# - --recursive: (optional) if 1 and --dir passed and not --json passed, will look inside --dir recursively and process .json files  
# - --json: (optional) full path to a .json file result from DocuVision to convert to csv. If given, --dir will be ignored
# - --loglevel: (optional) how verbose you want to logging to be to the csvconversion.log file
# Results:
# - will save a .csv file alongside each .json with the same filename stem
# - will append logging info to csvconversion.log in same dir as this script is run from


import os, logging, datetime, argparse, json
import pandas as pd
from pathlib import Path, PurePath

#get command line parameters passed
ap = argparse.ArgumentParser("docuvision_json_to_csv_conversion_sample", epilog="For help contact support@hank.ai") #, exit_on_error=False)
ap.add_argument("--json", help="Path to a .json file to convert to csv (use this OR --dir)", 
    default=None, type=str)
ap.add_argument("--dir", help="Full path to a directory on the current machine to process .json files in (use this or --json)", 
    default=".", type=str)
ap.add_argument("--splitonpid", help="if 1, will seperate .json based upon predicted unique patient encounter ids (pids) at {jsonfilename.stem}/{pid}.csv", 
    default=0, type=int)
ap.add_argument("--recursive", help="if 1 and --dir passed and not --json passed, will look inside --dir recursively and process .json files ", 
    default=0, type=int)
ap.add_argument("--loglevel", help="Logging level. Options are DEBUG, INFO, WARNING, ERROR, CRITICAL.", 
    default="DEBUG", type=str)

args, unknown = ap.parse_known_args()
if args.dir == '.': args.dir=os.path.dirname(os.path.realpath(__file__))
print("DOCUVISION SCRIPT STARTED")
print("args = ", args)

#setup logging
logging.basicConfig(
    filename='csvconversion.log', 
    filemode='a', 
    level=args.loglevel.upper(),
    format='%(asctime)s:%(levelname)s:%(message)s')
logging.info("STARTING PROCESSING. {}".format(datetime.datetime.now()))
logging.info("Processing dir={} json={} splitonpid={} recursive={}".format(args.dir, args.json, args.splitonpid, args.recursive))

if args.json is not None: 
    jsons = [args.json]
else: 
    dirP = Path(PurePath(Path.cwd(), args.dir))
    logging.debug(f"Dir to process {dirP}")
    if args.recursive:
        jsons = list(dirP.rglob("*.json"))
    else: 
        jsons = list(dirP.glob("*.json"))


#loop through all the json files in the directory running the script
for j in jsons:
    logging.info(f"Processing {j.name}")
    with open(j, 'r') as f:
        jstr = json.load(f) 
    resultobj = jstr.get('response').get('result').get('RESULT')
    metaobj = jstr.get('response').get('result').get('METADATA')
    origfn = Path(jstr.get('response').get('processedDocument').get('name')).name
    df = pd.DataFrame(resultobj) #create dataframe object from responses
    df['OriginDocumentName']=origfn
    df = df.sort_values(by=['OriginDocumentPage', 'label','confidence'], ascending=[1, 1, 0])
    #if 1: #not trypidcleanup: #don't allow out of order documents
    csvfn = j.parent / (j.stem+'.csv') 
    #create a single csv that holds ALL the identified labels and related information in the file
    df.to_csv(csvfn, index=False)
    logging.debug(f" Saved csv to {csvfn.name}")
    
    if args.splitonpid: # will put them here: /{filenamestem}/{pid}.csv
        for pid, grp in df.groupby('pid'):
            subdir = (j.parent / j.stem)
            subdir.mkdir(exist_ok=True)
            logging.debug(f" Made dir {subdir.name}")
            pidcsvfn = subdir / (pid+'.csv')
            pages = grp['OriginDocumentPage'].unique()
            grp.to_csv(pidcsvfn, index=False)
            logging.debug(f" Saved '{pid}' (pages={pages}) pidcsv to {pidcsvfn}")

    #want to know which pages had no extractions and which pages had no pid?
    missingPagenums = [x for x in range(1, metaobj.get('pagesProcessed')) if not x in df['OriginDocumentPage'].unique()]
    missingPIDs = df[pd.isnull(df['pid'])]['OriginDocumentPage'].unique()
    if len(missingPagenums)>0: logging.warning(f"Pages with no predictions: {missingPagenums}")
    if len(missingPIDs)>0: logging.warning(f"Pages with no pids: {missingPIDs}")
logging.info("DONE")
# %%
